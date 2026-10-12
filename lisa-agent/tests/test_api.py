from __future__ import annotations

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import threading
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from thelake_lisa import api


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(api, "DATA_DIR", tmp_path)
    monkeypatch.setattr(api, "SERVICE_TOKEN", "local-test-token")
    return TestClient(api.app)


def test_lisa_conversations_are_scoped_to_workspace(client):
    workspace_a = str(uuid4())
    workspace_b = str(uuid4())
    conversation_id = str(uuid4())
    headers = {"Authorization": "Bearer local-test-token"}

    created = client.post(
        "/internal/v1/conversations",
        headers=headers,
        json={"workspace_id": workspace_a, "conversation_id": conversation_id},
    )

    assert created.status_code == 200
    assert created.json()["conversation_id"] == conversation_id
    assert client.get(
        f"/internal/v1/conversations/{conversation_id}",
        headers=headers,
        params={"workspace_id": workspace_b},
    ).status_code == 404
    assert client.post(
        "/internal/v1/conversations/search",
        headers=headers,
        json={"workspace_id": workspace_b},
    ).json() == []


def test_lisa_internal_api_requires_service_token(client):
    response = client.post(
        "/internal/v1/conversations",
        json={"workspace_id": str(uuid4()), "conversation_id": str(uuid4())},
    )
    assert response.status_code == 401


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("post", "/internal/v1/conversations", {"workspace_id": "11111111-1111-4111-8111-111111111111", "conversation_id": "22222222-2222-4222-8222-222222222222"}),
        ("post", "/internal/v1/conversations/search", {"workspace_id": "11111111-1111-4111-8111-111111111111"}),
        ("post", "/internal/v1/scheduled-cycle", {"workspace_id": "11111111-1111-4111-8111-111111111111"}),
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


def test_user_chat_api_uses_the_runtime_workspace_without_accepting_workspace_selection(client, monkeypatch):
    workspace_id = str(uuid4())
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_LOCAL_ANONYMOUS", "1")
    response = client.post("/v1/conversations")
    assert response.status_code == 200
    created = response.json()
    assert created["workspace_id_hash"] == api.hashlib.sha256(workspace_id.encode()).hexdigest()
    assert client.post("/v1/conversations", json={"workspace_id": str(uuid4())}).status_code == 422


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
        encode = lambda value: base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
        header = encode({"alg": "HS256", "typ": "JWT"})
        claims = encode({"iss": "softprobe-edge", "aud": "sp-backend", "sub": "user-1", "workspace_id": scope})
        signing_input = f"{header}.{claims}"
        signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
        return f"{signing_input}.{base64.urlsafe_b64encode(signature).decode().rstrip('=')}"

    valid = client.get("/v1/conversations", headers={"X-Softprobe-Assertion": assertion(workspace_id)})
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
        "type": "event_callback", "team_id": "team-test", "event_id": "Ev-test",
        "event": {
            "type": "message", "channel": "channel-test", "user": "user-test",
            "ts": "1710000000.000001", "thread_ts": "1710000000.000000",
            "text": "Review this agent behavior.",
        },
    }
    body = json.dumps(event, separators=(",", ":")).encode()
    timestamp = str(int(api.datetime.now(api.UTC).timestamp()))
    digest = hmac.new(secret.encode(), b"v0:" + timestamp.encode() + b":" + body, hashlib.sha256).hexdigest()
    response = client.post(
        "/slack/events", content=body,
        headers={
            "Content-Type": "application/json", "X-Slack-Request-Timestamp": timestamp,
            "X-Slack-Signature": f"v0={digest}",
        },
    )
    assert response.status_code == 200
    receipt_path = api._slack_event_path(workspace_id, "Ev-test")
    assert json.loads(receipt_path.read_text())["status"] == "pending"

    captured = {}

    def fake_run_turn(**kwargs):
        captured.update(kwargs)
        from thelake_lisa.runtime import AgentTurn

        return AgentTurn(
            messages=[{"id": "reply-1", "role": "assistant", "text": "I will review it."}],
            activities=[],
        )

    posted = []
    monkeypatch.setattr(api, "run_turn", fake_run_turn)
    monkeypatch.setattr(api, "_post_slack_reply", lambda *args: posted.append(args))
    asyncio.run(api._process_slack_event(receipt_path))

    receipt = json.loads(receipt_path.read_text())
    assert receipt["status"] == "complete"
    assert captured["workspace_id"] == workspace_id
    assert captured["slack_channel_id"] == "channel-test"
    assert captured["slack_thread_ts"] == "1710000000.000000"
    assert posted == [("channel-test", "1710000000.000000", "I will review it.")]


def test_slack_event_rejects_an_invalid_signature(client, monkeypatch):
    monkeypatch.setenv("LISA_SLACK_SIGNING_SECRET", "secret")
    response = client.post(
        "/slack/events", content=b'{"type":"url_verification"}',
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
        from thelake_lisa.runtime import AgentTurn

        return AgentTurn(messages=[{"id": "assistant-1", "role": "assistant", "text": "Found it."}], activities=[])

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
        {"id": "assistant-1", "role": "assistant", "text": "Found it."}
    ]
    assert "chat-secret-value" not in captured["text"]
    assert "[REDACTED]" in captured["text"]
    assert captured["workspace_id"] == workspace_id
    assert captured["confirmed_policy_title"] is None


def test_explicit_chat_confirmation_is_bound_to_the_current_turn(client, monkeypatch):
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
        from thelake_lisa.runtime import AgentTurn

        return AgentTurn(messages=[], activities=[])

    monkeypatch.setattr(api, "run_turn", fake_run_turn)
    response = client.post(
        f"/internal/v1/conversations/{conversation_id}/messages",
        headers=headers,
        json={
            "workspace_id": workspace_id,
            "text": "Confirm policy rule: Refund eligibility",
            "consent": True,
        },
    )

    assert response.status_code == 200
    assert captured["confirmed_policy_title"] == "Refund eligibility"


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


def test_scheduled_cycle_runs_from_lisa_and_persists_its_turn(client, monkeypatch):
    workspace_id = str(uuid4())
    captured = {}

    def fake_run_turn(**kwargs):
        captured.update(kwargs)
        Path(kwargs["directory"]).mkdir(parents=True)
        (Path(kwargs["directory"]) / "conversation.json").write_text("temporary")
        from thelake_lisa.runtime import AgentTurn

        return AgentTurn(
            messages=[{"id": "scheduled-1", "role": "assistant", "text": "No new sample."}],
            activities=[{"id": "tool-1", "kind": "tool", "label": "Lisa used search_sessions"}],
        )

    monkeypatch.setattr(api, "run_turn", fake_run_turn)

    result = asyncio.run(api.run_scheduled_cycle(workspace_id))

    assert result["messages"][0]["text"] == "No new sample."
    assert captured["workspace_id"] == workspace_id
    assert "own-agent-qa" in captured["text"]
    assert "workspace tools" in captured["text"]
    assert "sample_agent_sessions" not in captured["text"]
    assert captured["conversation_id"] != api.scheduled_conversation_id(workspace_id)
    assert not Path(captured["directory"]).exists()
    saved = client.get(
        f"/internal/v1/conversations/{api.scheduled_conversation_id(workspace_id)}",
        headers={"Authorization": "Bearer local-test-token"},
        params={"workspace_id": workspace_id},
    )
    assert saved.status_code == 200
    assert saved.json()["messages"][0]["id"] == "scheduled-1"


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
    assert client.get(
        f"/internal/v1/conversations/{conversation_id}",
        headers=headers,
        params={"workspace_id": workspace_id},
    ).status_code == 404


def test_delete_cannot_be_undone_by_a_message_waiting_for_the_conversation_lock(client, monkeypatch):
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
    monkeypatch.setattr(api, "lock_for", lambda _directory: controlled_lock)
    monkeypatch.setattr(api, "run_turn", lambda **_kwargs: pytest.fail("deleted conversation ran"))

    def send_message():
        from fastapi.testclient import TestClient

        with TestClient(api.app) as send_client:
            return send_client.post(
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
    assert client.get(
        f"/internal/v1/conversations/{conversation_id}",
        headers=headers,
        params={"workspace_id": workspace_id},
    ).status_code == 404
