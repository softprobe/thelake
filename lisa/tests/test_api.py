from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from fastapi.testclient import TestClient

from lisa.app import app
from lisa.controllers import http as api
from lisa.repositories.session_traces import SessionTraceStore
from lisa.services.session_trace_intake import SessionTraceIntake


@pytest.fixture
def client(tmp_path, monkeypatch, lisa_test_database):
    workspace_id = str(uuid4())
    database = lisa_test_database(workspace_id)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_AGENT_WORKSPACE_ROOT", str(tmp_path / "agent-workspace"))
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    monkeypatch.setattr(api, "SERVICE_TOKEN", "local-test-token")
    with TestClient(app) as test_client:
        yield test_client


def _repository(client):
    return client.app.state.workspace_services.conversation_repository


def _service_headers():
    return {"Authorization": "Bearer local-test-token"}


def test_lisa_conversations_are_scoped_to_workspace(client):
    workspace_a = str(uuid4())
    workspace_b = str(uuid4())
    conversation_id = str(uuid4())
    headers = _service_headers()

    created = client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_a, "conversation_id": conversation_id},
    )

    assert created.status_code == 200
    assert created.json()["conversation_id"] == conversation_id
    assert (
        client.get(
            f"/internal/v1/conversations/{conversation_id}",
            headers=headers,
            params={"workspace_id": workspace_b},
        ).status_code
        == 404
    )
    assert (
        client.post(
            "/internal/v1/conversations/search",
            headers=headers,
            json={"workspace_id": workspace_b},
        ).json()
        == []
    )


def test_lisa_internal_api_requires_service_token(client):
    response = client.post(
        "/internal/v1/conversations",
        json={"workspace_id": str(uuid4()), "conversation_id": str(uuid4())},
    )
    assert response.status_code == 401


def _otlp_session(session_id="pushed-session-1"):
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "checkout-agent"}}
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "0123456789abcdef0123456789abcdef",
                                "spanId": "0123456789abcdef",
                                "name": "agent turn",
                                "attributes": [
                                    {"key": "sp.session.id", "value": {"stringValue": session_id}}
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def test_pushed_session_is_persisted_before_it_is_enqueued(client, tmp_path):
    class Queue:
        def __init__(self):
            self.messages = []
            self.sessions = set()

        def send(self, session_id, *message):
            if session_id in self.sessions:
                return False
            self.sessions.add(session_id)
            self.messages.append(message)
            return True

    services = client.app.state.workspace_services
    services.session_traces = SessionTraceStore(tmp_path)
    services.session_evaluation_queue = Queue()
    services.session_trace_intake = SessionTraceIntake(
        services.session_traces, services.session_evaluation_queue
    )

    response = client.post(
        "/internal/v1/session-traces", json=_otlp_session()
    )

    assert response.status_code == 202
    assert response.json() == {
        "session_id": "pushed-session-1",
        "agent_name": "checkout-agent",
        "status": "queued",
    }
    stored = services.session_traces.load("pushed-session-1")
    assert stored["span_count"] == 1
    assert len(services.session_evaluation_queue.messages) == 1
    duplicate = client.post(
        "/internal/v1/session-traces", json=_otlp_session()
    )
    assert duplicate.status_code == 202
    assert duplicate.json()["status"] == "already_queued"
    assert len(services.session_evaluation_queue.messages) == 1


def test_pushed_session_runs_through_postgres_queue_and_local_evaluator(client):
    services = client.app.state.workspace_services
    response = client.post("/internal/v1/session-traces", json=_otlp_session())

    assert response.status_code == 202
    activity_id = str(uuid5(
        NAMESPACE_URL,
        f"lisa:pushed-session:{services.context.workspace_id()}:pushed-session-1",
    ))
    deadline = threading.Event()
    for _ in range(100):
        activity = services.activity_store.get_activity_thread(activity_id)
        if activity and activity["activity"]["status"] in {"success", "error", "stopped"}:
            break
        deadline.wait(0.05)

    assert activity is not None
    assert activity["activity"]["status"] == "success"
    assert any(
        event.event_type == "policy_review_completed"
        for event in services.activity_store.list_events(activity_id)
    )


def test_pushed_session_rejects_oversized_and_malformed_requests(client, monkeypatch):
    services = client.app.state.workspace_services
    services.session_evaluation_queue = object()
    monkeypatch.setenv("LISA_MAX_SESSION_TRACE_BYTES", "32")
    headers = _service_headers()
    assert client.post(
        "/internal/v1/session-traces", headers=headers, json=_otlp_session()
    ).status_code == 413
    monkeypatch.setenv("LISA_MAX_SESSION_TRACE_BYTES", "1024")
    assert client.post(
        "/internal/v1/session-traces",
        headers={**headers, "Content-Type": "application/json"},
        content=b"not json",
    ).status_code == 400
    assert client.post(
        "/internal/v1/session-traces", headers=headers, content=b"{}"
    ).status_code == 415


def test_lisa_python_service_serves_its_frontend_and_client_routes(client, tmp_path, monkeypatch):
    static_dir = tmp_path / "static"
    (static_dir / "assets").mkdir(parents=True)
    (static_dir / "index.html").write_text(
        '<script>window.__LISA_API_BASE_PATH__="__LISA_API_BASE_PATH__";</script>',
        encoding="utf-8",
    )
    (static_dir / "assets" / "app.js").write_text("console.log('Lisa')", encoding="utf-8")
    monkeypatch.setenv("LISA_STATIC_DIR", str(static_dir))

    index = client.get("/lisa/")
    route = client.get("/lisa/activities/activity-1")
    asset = client.get("/lisa/assets/app.js")

    assert index.status_code == 200
    assert 'window.__LISA_API_BASE_PATH__="/v1"' in index.text
    assert route.status_code == 200 and route.text == index.text
    assert asset.status_code == 200 and "console.log" in asset.text
    assert client.get("/lisa/assets/missing.js").status_code == 404


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        (
            "post",
            "/internal/v1/conversations",
            {
                "workspace_id": "11111111-1111-4111-8111-111111111111",
                "conversation_id": "22222222-2222-4222-8222-222222222222",
            },
        ),
        (
            "post",
            "/internal/v1/conversations/search",
            {"workspace_id": "11111111-1111-4111-8111-111111111111"},
        ),
    ],
)
def test_lisa_internal_routes_reject_wrong_service_token(client, method, path, body):
    response = getattr(client, method)(
        path, headers={"Authorization": "Bearer wrong-token"}, json=body
    )
    assert response.status_code == 401


def test_lisa_internal_reads_reject_wrong_service_token(client):
    response = client.get(
        "/internal/v1/conversations/22222222-2222-4222-8222-222222222222",
        headers={"Authorization": "Bearer wrong-token"},
        params={"workspace_id": "11111111-1111-4111-8111-111111111111"},
    )
    assert response.status_code == 401


def test_user_chat_api_uses_the_runtime_workspace_without_accepting_workspace_selection(
    client, monkeypatch
):
    workspace_id = str(uuid4())
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    response = client.post("/v1/conversations")
    assert response.status_code == 200
    created = response.json()
    assert created["workspace_id_hash"] == sha256(workspace_id.encode()).hexdigest()
    assert client.post("/v1/conversations", json={"workspace_id": str(uuid4())}).status_code == 422


def test_user_chat_api_has_one_continuing_conversation_per_workspace(client, monkeypatch):
    workspace_id = str(uuid4())
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")

    first = client.post("/v1/conversations").json()
    second = client.post("/v1/conversations").json()

    assert second["conversation_id"] == first["conversation_id"]
    conversations = client.get("/v1/conversations").json()
    assert [item["conversation_id"] for item in conversations] == [first["conversation_id"]]


def test_user_chat_turn_passes_local_owner_identity_to_runtime(client, monkeypatch):
    from lisa.runtime import AgentTurn

    workspace_id = str(uuid4())
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    monkeypatch.setenv("LISA_LOCAL_ACTOR_ID", "e2e-owner")
    captured = {}

    def fake_run_turn(**kwargs):
        captured.update(kwargs)
        return AgentTurn(messages=[{"id": "assistant-1", "role": "assistant", "text": "Done."}], activities=[])

    monkeypatch.setattr(api, "run_turn", fake_run_turn)
    conversation_id = client.post("/v1/conversations").json()["conversation_id"]
    response = client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "Create a policy", "consent": True},
    )

    assert response.status_code == 200
    assert captured["actor_id"] == "e2e-owner"
    assert captured["turn_source"] == "web"


def test_user_chat_api_requires_a_valid_workspace_assertion_outside_local_mode(client, monkeypatch):
    monkeypatch.setenv("LISA_WORKSPACE_ID", str(uuid4()))
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "0")
    response = client.get("/v1/conversations")
    assert response.status_code == 401


def test_user_chat_api_accepts_only_a_signed_assertion_for_its_workspace(client, monkeypatch):
    workspace_id = str(uuid4())
    secret = "test-assertion-secret"
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "0")
    monkeypatch.setenv("SOFTPROBE_ASSERTION_HMAC_SECRET", secret)

    def assertion(scope: str) -> str:
        encode = lambda value: (
            base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
        )
        header = encode({"alg": "HS256", "typ": "JWT"})
        claims = encode(
            {"iss": "softprobe-edge", "aud": "sp-backend", "sub": "user-1", "workspace_id": scope}
        )
        signing_input = f"{header}.{claims}"
        signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
        return f"{signing_input}.{base64.urlsafe_b64encode(signature).decode().rstrip('=')}"

    valid = client.get(
        "/v1/conversations", headers={"X-Softprobe-Assertion": assertion(workspace_id)}
    )
    other_workspace = client.get(
        "/v1/conversations", headers={"X-Softprobe-Assertion": assertion(str(uuid4()))}
    )
    assert valid.status_code == 200
    assert other_workspace.status_code == 401


def test_slack_event_is_processed_by_lisa_in_its_original_thread(client, monkeypatch):
    workspace_id = str(uuid4())
    secret = "slack-signing-test"
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_SLACK_SIGNING_SECRET", secret)
    monkeypatch.setenv("LISA_SLACK_TEAM_ID", "team-test")
    monkeypatch.setenv("LISA_SLACK_ALLOWED_USERS", "user-test")
    event = {
        "type": "event_callback",
        "team_id": "team-test",
        "event_id": "Ev-test",
        "event": {
            "type": "message",
            "channel": "channel-test",
            "user": "user-test",
            "ts": "1710000000.000001",
            "thread_ts": "1710000000.000000",
            "text": "Approve policy: P1.2.1",
        },
    }
    body = json.dumps(event, separators=(",", ":")).encode()
    timestamp = str(int(api.datetime.now(api.UTC).timestamp()))
    digest = hmac.new(
        secret.encode(), b"v0:" + timestamp.encode() + b":" + body, hashlib.sha256
    ).hexdigest()
    response = client.post(
        "/slack/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Slack-Request-Timestamp": timestamp,
            "X-Slack-Signature": f"v0={digest}",
        },
    )
    assert response.status_code == 200
    services = client.app.state.workspace_services
    receipt_path = services.slack_event_service.events.event_path(workspace_id, "Ev-test")
    assert json.loads(receipt_path.read_text())["status"] == "pending"

    captured = {}

    def fake_run_turn(**kwargs):
        captured.update(kwargs)
        from lisa.runtime import AgentTurn

        return AgentTurn(
            messages=[{"id": "reply-1", "role": "assistant", "text": "I will review it."}],
            activities=[],
        )

    from lisa.services.slack_events import SlackEventWorker

    class FakeSlackClient:
        def post_message(self, channel, text, thread_ts, *, timeout):
            posted.append((channel, thread_ts, text))

    posted = []
    worker = SlackEventWorker(
        services.slack_event_service,
        FakeSlackClient(),
        fake_run_turn,
    )
    asyncio.run(worker._process(receipt_path))

    receipt = json.loads(receipt_path.read_text())
    assert receipt["status"] == "complete"
    assert captured["text"] == "Approve policy: P1.2.1"
    assert "confirmed_policy_id" not in captured
    assert captured["workspace_id"] == workspace_id
    assert captured["slack_channel_id"] == "channel-test"
    assert captured["slack_thread_ts"] == "1710000000.000000"
    assert posted == [("channel-test", "1710000000.000000", "I will review it.")]


def test_slack_event_rejects_an_invalid_signature(client, monkeypatch):
    monkeypatch.setenv("LISA_SLACK_SIGNING_SECRET", "secret")
    response = client.post(
        "/slack/events",
        content=b'{"type":"url_verification"}',
        headers={"X-Slack-Request-Timestamp": "1", "X-Slack-Signature": "v0=bad"},
    )
    assert response.status_code == 401


def test_lisa_rejects_user_chosen_workspace_fields(client):
    response = client.post(
        "/internal/v1/conversations/search",
        headers={"Authorization": "Bearer local-test-token"},
        json={"workspace_id": str(uuid4()), "other_workspace_id": str(uuid4())},
    )
    assert response.status_code == 422


def test_lisa_saves_agent_reply_after_openhands_run(client, monkeypatch):
    workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    created = client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )
    assert created.status_code == 200

    captured = {}

    def fake_run_turn(**kwargs):
        captured.update(kwargs)
        from lisa.runtime import AgentTurn

        return AgentTurn(
            messages=[{"id": "assistant-1", "role": "assistant", "text": "Found it."}],
            activities=[],
        )

    monkeypatch.setattr(api, "run_turn", fake_run_turn)

    response = client.post(
        f"/internal/v1/conversations/{conversation_id}/messages",
        headers=headers,
        json={
            "workspace_id": workspace_id,
            "text": "Find recent failures and keep api_key=chat-secret-value.",
            "consent": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["messages"] == [
        {
            "id": "assistant-1",
            "role": "assistant",
            "text": "Found it.",
            "turn_source": "web",
        }
    ]
    assert "chat-secret-value" not in captured["text"]
    assert "[REDACTED]" in captured["text"]
    assert captured["workspace_id"] == workspace_id
    assert "confirmed_policy_id" not in captured


def test_cancelled_chat_waits_for_openhands_and_persists_its_turn(client, monkeypatch):
    workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )
    started = threading.Event()
    finish = threading.Event()

    def fake_run_turn(**_kwargs):
        started.set()
        assert finish.wait(timeout=5)
        from lisa.runtime import AgentTurn

        return AgentTurn(
            messages=[{"id": "cancelled-turn", "role": "assistant", "text": "Saved."}],
            activities=[],
        )

    monkeypatch.setattr(api, "run_turn", fake_run_turn)

    async def send_then_cancel():
        services = client.app.state.workspace_services
        task = asyncio.create_task(
            services.conversations.run_user_turn(
                conversation_id,
                workspace_id,
                "Continue",
                run_turn=api.run_turn,
            )
        )
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        repository = _repository(client)
        assert repository.lock_for(
            repository.conversation_directory(workspace_id, conversation_id)
        ).locked()
        finish.set()
        return await task

    result = asyncio.run(send_then_cancel())
    assert result["messages"][0]["id"] == "cancelled-turn"
    saved = client.get(
        f"/internal/v1/conversations/{conversation_id}",
        headers=headers,
        params={"workspace_id": workspace_id},
    ).json()
    assert saved["messages"][0]["id"] == "cancelled-turn"


def test_chat_message_reaches_the_agent_without_a_bound_policy(client, monkeypatch):
    workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )
    captured = {}

    def fake_run_turn(**kwargs):
        captured.update(kwargs)
        from lisa.runtime import AgentTurn

        return AgentTurn(messages=[], activities=[])

    monkeypatch.setattr(api, "run_turn", fake_run_turn)
    response = client.post(
        f"/internal/v1/conversations/{conversation_id}/messages",
        headers=headers,
        json={
            "workspace_id": workspace_id,
            "text": "Approve policy: P1.2.1",
            "consent": True,
        },
    )

    assert response.status_code == 200
    assert captured["text"] == "Approve policy: P1.2.1"
    assert "confirmed_policy_id" not in captured

def test_activity_feed_http_endpoints_use_workspace_database(
    monkeypatch, lisa_test_database, tmp_path
):
    from fastapi.testclient import TestClient

    from lisa.app import app

    database = lisa_test_database()
    workspace_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    monkeypatch.setenv("LISA_OWNER_USER_IDS", "local")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.delenv("LISA_AGENT_BACKEND", raising=False)
    with TestClient(app) as test_client:
        services = test_client.app.state.workspace_services
        activity = services.activity_store.create_activity(
            title="Inspect refund session", source="web",
        )
        evaluation = services.activity_store.create_activity(
            title="Evaluate refund session", source="web",
            kind="policy_evaluation",
        )
        services.activity_store.record_conversation_trajectory(
            activity.id,
            [{"id": "api-message", "role": "user", "text": "Inspect refund session."}],
            [{"id": "api-runtime-event", "kind": "tool", "tool_name": "find_activity", "sequence": 2}],
        )
        listed = test_client.get("/v1/activities?open_only=true").json()
        evaluations = test_client.get(
            "/v1/activities?activity_type=policy_evaluation"
        ).json()
        detail = test_client.get(f"/v1/activities/{activity.id}").json()
    assert any(item["id"] == activity.id for item in listed)
    assert [item["id"] for item in evaluations] == [evaluation.id]
    assert detail["activity"]["title"] == "Inspect refund session"
    assert [item["title"] for item in detail["actions"]] == [
        "Inspect refund session", "You: Inspect refund session.", "Lisa used find_activity",
    ]
    assert {item["reference_id"] for item in detail["references"]} >= {
        "api-message", "api-runtime-event",
    }


def test_activity_feed_and_detail_require_an_authorized_owner(
    monkeypatch, lisa_test_database, tmp_path
):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    database = lisa_test_database(workspace_id)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    monkeypatch.setenv("LISA_LOCAL_ACTOR_ID", "workspace-member")
    monkeypatch.setenv("LISA_OWNER_USER_IDS", "workspace-owner")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    with TestClient(app) as test_client:
        services = test_client.app.state.workspace_services
        activity = services.activity_store.create_activity(
            title="Private policy evidence", source="web", kind="policy_evaluation",
        )

        assert test_client.get("/v1/activities").status_code == 403
        assert test_client.get(f"/v1/activities/{activity.id}").status_code == 403


def test_user_policy_list_is_authenticated_and_bounded(
    lisa_test_database, monkeypatch, tmp_path
):
    from fastapi.testclient import TestClient

    from lisa.app import app

    workspace_id = "11111111-1111-4111-8111-111111111111"
    database = lisa_test_database(workspace_id)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    with TestClient(app) as test_client:
        repository = test_client.app.state.workspace_services.policy_service.repository
        repository.seed_scope(
            "workspace",
            "",
            [],
            [
                {
                    "name": f"Rule {index:03}",
                    "criteria": "Check the required evidence first.",
                    "approved": index % 2 == 0,
                }
                for index in range(1, 102)
            ],
        )

        response = test_client.get("/v1/policies?limit=100&offset=0")
        final_page = test_client.get("/v1/policies?limit=100&offset=100")
        assert response.status_code == 200
        assert len(response.json()["items"]) == 100
        assert response.json()["next_offset"] == 100
        assert response.json()["items"][0]["name"] == "Rule 001"
        assert final_page.status_code == 200
        assert len(final_page.json()["items"]) == 1
        assert final_page.json()["next_offset"] is None
        assert test_client.get("/v1/policies?offset=10001").status_code == 400

        monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "0")
        assert test_client.get("/v1/policies").status_code == 401


def test_policy_suggestion_feed_owner_approval_and_dismissal(
    lisa_test_database, monkeypatch, tmp_path
):
    from fastapi.testclient import TestClient

    from lisa.app import app
    from lisa.models.policy_discovery import PolicySuggestionDraft

    workspace_id = str(uuid4())
    database = lisa_test_database(workspace_id)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    monkeypatch.setenv("LISA_LOCAL_ACTOR_ID", "owner-1")
    monkeypatch.setenv("LISA_OWNER_USER_IDS", "owner-1")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    with TestClient(app) as test_client:
        discovery = test_client.app.state.workspace_services.policy_discovery
        signal_rows = []
        for session_id in ("source-1", "source-2", "source-3"):
            activity = test_client.app.state.workspace_services.activity_store.create_activity(
                title=f"Review {session_id}", source="session_push", kind="session_evaluation",
            )
            assert discovery.reserve_sample(
                session_id=session_id, agent_name="agent", activity_id=activity.id,
            )
            discovery.repository.save_signal(
                session_id=session_id, activity_id=activity.id, agent_name="agent",
                summary="A destructive action lacked confirmation.",
                span_ids=[f"span-{session_id}"],
                evidence=[{"span_id": f"span-{session_id}", "kind": "user_message", "content": "Delete my account"}],
            )
        observation_update = next(
            item for item in test_client.get("/v1/proactive-updates?limit=10").json()["items"]
            if item["source_type"] == "qa_observation"
        )
        assert observation_update["messages"] == ["A destructive action lacked confirmation."]
        observation = test_client.get(
            f"/v1/qa-observations/{observation_update['source_id']}"
        )
        assert observation.status_code == 200
        assert observation.json()["session_id"] in {"source-1", "source-2", "source-3"}
        assert observation.json()["evidence"][0]["span_id"].startswith("span-source-")
        signal_rows = discovery.repository.pending_signals(limit=10)
        drafts = [PolicySuggestionDraft(
            scope="workspace", name="Confirm destructive actions",
            criteria="Ask for explicit confirmation before irreversible actions.",
            rationale="The cited user requests and tool action show no confirmation step.",
            signal_ids=[str(row["id"]) for row in signal_rows],
        )]
        created = discovery.repository.create_suggestions(
            drafts=drafts, signal_rows=signal_rows, overlaps_by_draft=[[]]
        )
        suggestion_id = created[0]["id"]
        duplicate = discovery.repository.create_suggestions(
            drafts=drafts, signal_rows=signal_rows, overlaps_by_draft=[[]]
        )
        assert duplicate[0]["id"] == suggestion_id

        feed = test_client.get("/v1/policy-suggestions")
        assert feed.status_code == 200
        assert feed.json()["can_review"] is True
        assert feed.json()["items"][0]["id"] == suggestion_id
        updates = test_client.get("/v1/proactive-updates?limit=10")
        assert updates.status_code == 200
        suggestion_updates = [
            item for item in updates.json()["items"]
            if item["source_type"] == "policy_suggestion"
        ]
        assert len(suggestion_updates) == 1
        assert suggestion_updates[0]["source_id"] == suggestion_id
        assert suggestion_updates[0]["messages"] == [
            "The cited user requests and tool action show no confirmation step."
        ]
        update_id = suggestion_updates[0]["id"]
        detail = test_client.get(f"/v1/policy-suggestions/{suggestion_id}")
        assert detail.status_code == 200
        cited = {
            evidence["span_id"]
            for source in detail.json()["evidence"]
            for evidence in source["evidence"]
        }
        assert cited == {"span-source-1", "span-source-2", "span-source-3"}
        approved = test_client.post(
            f"/v1/policy-suggestions/{suggestion_id}/approve",
            json={"name": "Confirm before irreversible actions", "criteria": "Ask the user for explicit confirmation before irreversible actions."},
        )
        assert approved.status_code == 200
        assert approved.json()["policy"]["approved"] is True
        approved_updates = test_client.get("/v1/proactive-updates?limit=10")
        assert approved_updates.status_code == 200
        assert next(
            item for item in approved_updates.json()["items"]
            if item["source_id"] == suggestion_id
        )["id"] == update_id

        second = discovery.repository.create_suggestions(
            drafts=[drafts[0].model_copy(update={"name": "Confirm before export"})],
            signal_rows=signal_rows, overlaps_by_draft=[[]],
        )[0]
        monkeypatch.setenv("LISA_LOCAL_ACTOR_ID", "not-an-owner")
        assert test_client.post(
            f"/v1/policy-suggestions/{second['id']}/approve", json={}
        ).status_code == 403
        assert test_client.get("/v1/policy-suggestions").status_code == 403
        assert test_client.get(f"/v1/policy-suggestions/{second['id']}").status_code == 403
        assert test_client.get("/v1/proactive-updates").status_code == 403
        assert test_client.get(
            f"/v1/qa-observations/{observation_update['source_id']}"
        ).status_code == 403
        monkeypatch.setenv("LISA_LOCAL_ACTOR_ID", "owner-1")
        dismissed = test_client.post(
            f"/v1/policy-suggestions/{second['id']}/dismiss", json={}
        )
        assert dismissed.status_code == 200
        assert dismissed.json()["status"] == "dismissed"
        assert test_client.get("/v1/policy-suggestions?status=dismissed").json()["items"][0]["id"] == second["id"]
        update_refs = test_client.get("/v1/proactive-updates?limit=10").json()["items"]
        assert len([item for item in update_refs if item["source_id"] == second["id"]]) == 1


def test_lisa_closes_openhands_conversation_when_run_fails(client, monkeypatch):
    workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )

    def fake_run_turn(**_kwargs):
        raise RuntimeError("model unavailable")

    monkeypatch.setattr(api, "run_turn", fake_run_turn)
    response = client.post(
        f"/internal/v1/conversations/{conversation_id}/messages",
        headers=headers,
        json={
            "workspace_id": workspace_id,
            "text": "Find recent failures.",
            "consent": True,
        },
    )

    assert response.status_code == 503


def test_lisa_requires_provider_data_consent(client):
    workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )

    response = client.post(
        f"/internal/v1/conversations/{conversation_id}/messages",
        headers=headers,
        json={"workspace_id": workspace_id, "text": "Review sessions."},
    )

    assert response.status_code == 422


def test_lisa_conversation_can_only_be_deleted_by_its_workspace(client):
    workspace_id = str(uuid4())
    other_workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )

    other_delete = client.delete(
        f"/internal/v1/conversations/{conversation_id}",
        headers=headers,
        params={"workspace_id": other_workspace_id},
    )
    owner_delete = client.delete(
        f"/internal/v1/conversations/{conversation_id}",
        headers=headers,
        params={"workspace_id": workspace_id},
    )

    assert other_delete.status_code == 404
    assert owner_delete.status_code == 200
    assert (
        client.get(
            f"/internal/v1/conversations/{conversation_id}",
            headers=headers,
            params={"workspace_id": workspace_id},
        ).status_code
        == 404
    )


def test_delete_cannot_be_undone_by_a_message_waiting_for_the_conversation_lock(
    client, monkeypatch
):
    workspace_id = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}
    client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_id, "conversation_id": conversation_id},
    )

    send_waiting = threading.Event()
    allow_send = threading.Event()

    class SendPausedLock:
        entries = 0
        guard = threading.Lock()

        async def __aenter__(self):
            with self.guard:
                self.entries += 1
                entry = self.entries
            if entry == 1:
                send_waiting.set()
                await asyncio.to_thread(allow_send.wait, 10)

        async def __aexit__(self, *_args):
            return False

    controlled_lock = SendPausedLock()
    monkeypatch.setattr(
        _repository(client), "lock_for", lambda _directory: controlled_lock
    )
    monkeypatch.setattr(api, "run_turn", lambda **_kwargs: pytest.fail("deleted conversation ran"))

    def send_message():
        return client.post(
            f"/internal/v1/conversations/{conversation_id}/messages",
            headers=headers,
            json={"workspace_id": workspace_id, "text": "Continue", "consent": True},
        )

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending_send = pool.submit(send_message)
        assert send_waiting.wait(5), "send did not reach the conversation lock"
        deleted = client.delete(
            f"/internal/v1/conversations/{conversation_id}",
            headers=headers,
            params={"workspace_id": workspace_id},
        )
        allow_send.set()
        sent = pending_send.result(timeout=10)

    assert deleted.status_code == 200
    assert sent.status_code == 404
    assert (
        client.get(
            f"/internal/v1/conversations/{conversation_id}",
            headers=headers,
            params={"workspace_id": workspace_id},
        ).status_code
        == 404
    )
