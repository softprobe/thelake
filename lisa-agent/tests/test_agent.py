from uuid import UUID
from pathlib import Path
import io

import pytest

from thelake_lisa.backends import openhands as agent


def test_build_conversation_uses_sdk_conversation_id_type(monkeypatch, tmp_path):
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    captured = {}

    monkeypatch.setattr(agent, "LLM", lambda **kwargs: object())
    monkeypatch.setattr(agent, "Agent", lambda **kwargs: object())

    def conversation_factory(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(agent, "Conversation", conversation_factory)
    conversation_id = "be3c93b1-d225-4ce7-8dff-06ac2924289f"

    agent.build_conversation(
        conversation_id,
        str(tmp_path),
        workspace_id="11111111-1111-4111-8111-111111111111",
    )

    assert captured["conversation_id"] == UUID(conversation_id)
    assert captured["visualizer"] is None


def test_redacts_credentials_in_user_messages_before_they_are_persisted():
    text = 'Please inspect this: api_key="message-secret-value" and Bearer abcdefghijklmnopqrstuvwxyz'

    redacted = agent.redact_text(text)

    assert "message-secret-value" not in redacted
    assert "abcdefghijklmnopqrstuvwxyz" not in redacted
    assert "api_key" in redacted
    assert "[REDACTED]" in redacted


def test_redacts_bearer_values_inside_authorization_fields():
    redacted = agent.redact_text("authorization: Bearer telemetry-secret-value")

    assert "telemetry-secret-value" not in redacted
    assert "authorization: [REDACTED]" == redacted


def test_redacts_credentials_in_json_chat_messages():
    redacted = agent.redact_text('{"api_key": "arbitrary-json-secret"}')

    assert "arbitrary-json-secret" not in redacted
    assert redacted == '{"api_key": "[REDACTED]"}'


def test_redacts_credentials_in_composite_json_keys():
    text = '{"AWS_SECRET_ACCESS_KEY": "aws-composite-secret", "OPENAI_API_KEY": "openai-composite-secret"}'

    redacted = agent.redact_text(text)

    assert "aws-composite-secret" not in redacted
    assert "openai-composite-secret" not in redacted
    assert '"AWS_SECRET_ACCESS_KEY": "[REDACTED]"' in redacted
    assert '"OPENAI_API_KEY": "[REDACTED]"' in redacted


def test_redacts_credentials_in_camel_case_json_keys():
    redacted = agent.redact_text('{"clientSecret": "camel-case-secret", "accessToken": "camel-token-secret"}')

    assert "camel-case-secret" not in redacted
    assert "camel-token-secret" not in redacted
    assert '"clientSecret": "[REDACTED]"' in redacted
    assert '"accessToken": "[REDACTED]"' in redacted


def test_redacts_json_credentials_embedded_in_prose():
    redacted = agent.redact_text('Inspect this config: {"clientSecret": "prose-json-secret"}.')

    assert "prose-json-secret" not in redacted
    assert '"clientSecret": "[REDACTED]"' in redacted



def test_lisa_agent_uses_openhands_skills_and_mcp_config(monkeypatch, tmp_path):
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake:8090")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "test-token")
    monkeypatch.setenv("GOOGLE_API_KEY", "judge-test-key")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "slack-test-token")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "channel-test")
    monkeypatch.setenv("LISA_SKILL_DIR", str(Path(__file__).parents[2] / "skills"))
    captured = {}

    monkeypatch.setattr(agent, "LLM", lambda **kwargs: object())
    monkeypatch.setattr(agent, "Agent", lambda **kwargs: captured.update(kwargs) or object())
    monkeypatch.setattr(agent, "Conversation", lambda **kwargs: object())

    agent.build_conversation(
        "be3c93b1-d225-4ce7-8dff-06ac2924289f",
        str(tmp_path / "workspace" / "conversation"),
        workspace_id="11111111-1111-4111-8111-111111111111",
    )

    assert captured["tools"] == []
    assert "lisa" in captured["mcp_config"]
    lisa_mcp = captured["mcp_config"]["lisa"]
    assert lisa_mcp.args == ["-m", "thelake_lisa.mcp_tools"]
    assert lisa_mcp.env["LISA_WORKSPACE_ID"].get_secret_value() == "11111111-1111-4111-8111-111111111111"
    assert lisa_mcp.env["GOOGLE_API_KEY"].get_secret_value() == "judge-test-key"
    assert lisa_mcp.env["LISA_SLACK_BOT_TOKEN"].get_secret_value() == "slack-test-token"
    assert lisa_mcp.env["LISA_SLACK_CHANNEL_ID"].get_secret_value() == "channel-test"
    skill_names = {skill.name for skill in captured["agent_context"].skills}
    assert {"learn-agent-policy", "review-agent-policy", "investigate-evaluator-failure"} <= skill_names


def test_lisa_mcp_exposes_policy_session_and_evaluator_tools(tmp_path, monkeypatch):
    import asyncio
    from fastmcp import Client
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake:8090")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "test-token")

    async def list_tools():
        async with Client(mcp_tools.mcp) as client:
            return {tool.name: tool for tool in await client.list_tools()}

    tools = asyncio.run(list_tools())

    assert {
        "list_policies", "read_policy", "update_policy", "search_sessions",
        "list_active_evaluations", "evaluate_session", "notify_evaluation_failure",
        "create_evaluation_draft", "activate_evaluation",
    } <= tools.keys()
    assert "sample_agent_sessions" not in tools
    assert "confirmed_rule_title" not in tools["update_policy"].inputSchema["properties"]
    assert "openhands" not in Path(mcp_tools.__file__).read_text(encoding="utf-8").lower()


def test_slack_conversation_binds_evaluation_draft_to_the_same_thread(monkeypatch, tmp_path):
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "channel-test")
    monkeypatch.setenv("LISA_SLACK_THREAD_TS", "1710000000.000000")
    monkeypatch.setenv("LISA_TURN_SOURCE", "slack")
    monkeypatch.setenv("LISA_EVALUATION_APPROVAL_TITLE", "Eligibility")
    monkeypatch.setenv("LISA_EVALUATION_APPROVAL_AGENT", "support-agent")
    policy = "# Workspace policy\n\n## Eligibility\n\nCheck the requested policy.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Eligibility")
    mcp_tools.update_policy(policy, "")
    captured = {}

    def fake_lake_post(path, body):
        captured["path"] = path
        captured["body"] = body
        return {"evaluator_id": "check-1", "version": 1, "name": "Eligibility", "target_agent_name": "support-agent"}

    monkeypatch.setattr(mcp_tools, "_thelake_post", fake_lake_post)
    result = mcp_tools.create_evaluation_draft("support-agent", "Eligibility")
    assert result == {
        "name": "Eligibility",
        "target_agent_name": "support-agent",
        "status": "draft",
        "criteria": "## Eligibility\n\nCheck the requested policy.",
    }
    assert captured["body"]["evaluator"]["criteria"] == result["criteria"]
    assert captured["body"]["evaluator"]["evaluator_id"] == mcp_tools._evaluation_id("support-agent", "Eligibility")
    assert "evaluator_id" not in result
    assert captured["path"] == "/internal/v1/lisa/evaluators"
    assert captured["body"]["slack_channel_id"] == "channel-test"
    assert captured["body"]["slack_thread_ts"] == "1710000000.000000"


def test_thelake_api_errors_include_safe_response_details(monkeypatch):
    from urllib.error import HTTPError
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake.test")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "unit-test-token")

    def reject_request(*_args, **_kwargs):
        raise HTTPError(
            "http://thelake.test/internal/v1/lisa/evaluators",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"error":"evaluator_id contains invalid characters"}'),
        )

    monkeypatch.setattr(mcp_tools.urllib.request, "urlopen", reject_request)

    with pytest.raises(RuntimeError, match="400.*evaluator_id contains invalid characters"):
        mcp_tools._thelake_post("/internal/v1/lisa/evaluators", {})


def test_policy_tools_scope_files_per_workspace(tmp_path, monkeypatch):
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    first = "11111111-1111-4111-8111-111111111111"
    second = "22222222-2222-4222-8222-222222222222"
    monkeypatch.setenv("LISA_WORKSPACE_ID", first)
    current = mcp_tools.read_policy()
    draft = "# Workspace policy\n\n## Refund Eligibility Policy\n\nCheck eligibility before refunding.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund Eligibility Policy")

    mcp_tools.update_policy(draft, current)

    assert mcp_tools.read_policy() == draft
    monkeypatch.setenv("LISA_WORKSPACE_ID", second)
    assert mcp_tools.read_policy() == "No policy exists yet."


def test_policy_update_requires_owner_confirmation_to_add_rule(tmp_path, monkeypatch):
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    current = "# Workspace policy\n\n## Refund eligibility\n\nCheck eligibility before refunding.\n"

    import pytest
    with pytest.raises(ValueError, match="explicit confirmation"):
        mcp_tools.update_policy(current, "")

    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    mcp_tools.update_policy(current, "")


def test_policy_update_requires_confirmation_to_change_a_confirmed_rule(tmp_path, monkeypatch):
    from thelake_lisa import mcp_tools
    import pytest

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    current = "# Workspace policy\n\n## Refund eligibility\n\nCheck eligibility before refunding.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    mcp_tools.update_policy(current, "")

    changed = current.replace("Check eligibility before refunding", "Check the ticket before refunding")
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_TITLE")
    with pytest.raises(ValueError, match="explicit confirmation"):
        mcp_tools.update_policy(changed, current)

    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    mcp_tools.update_policy(changed, current)


def test_policy_tools_redact_secrets_before_persisting_markdown(tmp_path, monkeypatch):
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    current = mcp_tools.read_policy()
    draft = '# Workspace policy\n\n## Support credential\n\nKeep credential data private. api_key="policy-secret-value"\n'
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Support credential")

    mcp_tools.update_policy(draft, current)
    saved = mcp_tools.read_policy()

    assert "policy-secret-value" not in saved
    assert "[REDACTED]" in saved


def test_policy_update_rejects_empty_rule_markdown(tmp_path, monkeypatch):
    import pytest
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    with pytest.raises(ValueError, match="readable Markdown"):
        mcp_tools.update_policy(
            "# Workspace policy\n\n## Inferred rule\n\n",
            "",
        )


def test_evaluate_session_runs_local_judge_and_persists_score(monkeypatch):
    from thelake_lisa import mcp_tools

    workspace_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    calls = []
    evaluator = {
        "evaluator_id": "refund-check",
        "version": 2,
        "target_agent_name": "support-agent",
        "name": "Refund eligibility",
        "criteria": "Check eligibility before a refund.",
        "threshold": 0.7,
        "uncertainty_margin": 0.1,
        "required_tool_order": [{"before": "eligibility", "action": "refund", "require_result_before_action": True}],
        "slack_channel_id": None,
    }

    def fake_lake_post(path, body, *, include_status=False):
        calls.append((path, body))
        if path.endswith("evaluators/active"):
            return {"items": [evaluator]}
        if path.endswith("sessions/evidence"):
            return {"session_id": "session-1", "agent_names": ["support-agent"], "scores": [], "evidence_limited": False, "spans": [
                {"span_id": "span-1", "trace_id": "trace-1", "start_time": "2026-10-09T12:00:00Z",
                 "end_time": "2026-10-09T12:00:01Z", "observation_type": "generation",
                 "input": "Refund this ticket", "output": "Checking eligibility."}
            ]}
        if path.endswith("/scores"):
            result = {"score_id": body["score_id"]}
            return (result, 201) if include_status else result
        raise AssertionError(path)

    monkeypatch.setattr(mcp_tools, "_thelake_post", fake_lake_post)
    evaluations = []

    def fake_evaluate(request):
        evaluations.append(request)
        return {
            "status": "fail", "score": 0.2, "rationale": "Refund preceded eligibility.",
            "framework": "deepeval", "evidence": [], "limitations": [],
        }

    monkeypatch.setattr(mcp_tools, "run_evaluation", fake_evaluate)
    result = mcp_tools.evaluate_session("support-agent", "Refund eligibility", "session-1")

    evaluation_request = evaluations[0]
    stored_score = calls[2][1]
    assert result["status"] == "fail"
    assert evaluation_request["evaluator"]["version"] == 2
    assert evaluation_request["evidence"]["events"][0]["kind"] == "user_message"
    assert stored_score["string_value"] == "fail"
    assert stored_score["config_id"] == "evaluator:refund-check:v2"
    assert "notification" not in result


def test_local_evaluator_keeps_trace_size_limits_and_hides_provider_errors(monkeypatch):
    from thelake_lisa import mcp_tools

    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    payload = {
        "run_id": "bounded-test",
        "evaluator": {
            "evaluator_id": "refund-check", "version": 1,
            "name": "Refund eligibility", "criteria": "Check eligibility first.",
        },
        "evidence": {
            "trace_id": "trace-test", "complete": True,
            "tool_events_captured": False, "events": [
                {"sequence": index, "span_id": f"span-{index}",
                 "timestamp": "2026-10-09T12:00:00Z", "kind": "user_message",
                 "content": "x" * 20_000}
                for index in range(7)
            ],
        },
    }

    oversized = mcp_tools.run_evaluation(payload)
    assert oversized["status"] == "error"
    assert oversized["limitations"] == ["evaluation_request_or_provider_error"]
    assert "x" * 100 not in oversized["rationale"]

    payload["evidence"]["events"] = [
        {"sequence": 0, "span_id": "user", "timestamp": "2026-10-09T12:00:00Z",
         "kind": "user_message", "content": "Refund this ticket."},
        {"sequence": 1, "span_id": "assistant", "timestamp": "2026-10-09T12:00:01Z",
         "kind": "assistant_message", "content": "I will check eligibility."},
    ]
    missing_key = mcp_tools.run_evaluation(payload)
    assert missing_key["status"] == "error"
    assert "credentials" not in missing_key["rationale"].lower()


def test_evaluate_session_skips_another_agent_and_existing_score(monkeypatch):
    import hashlib

    from thelake_lisa import mcp_tools

    workspace_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    evaluator = {
        "evaluator_id": "refund-check", "version": 2,
        "target_agent_name": "support-agent", "name": "Refund eligibility",
        "criteria": "Check eligibility before a refund.",
    }
    evaluation_calls = []

    def fake_lake_post(path, body, *, include_status=False):
        if path.endswith("evaluators/active"):
            return {"items": [evaluator]}
        if path.endswith("sessions/evidence"):
            return {
                "session_id": "session-1", "agent_names": ["another-agent"],
                "scores": [], "spans": [{"trace_id": "trace-1", "observation_type": "generation", "input": "hello"}],
            }
        raise AssertionError(path)

    monkeypatch.setattr(mcp_tools, "_thelake_post", fake_lake_post)
    monkeypatch.setattr(mcp_tools, "run_evaluation", lambda request: evaluation_calls.append(request))
    mismatch = mcp_tools.evaluate_session("support-agent", "Refund eligibility", "session-1")
    assert mismatch["status"] == "skipped_agent_mismatch"
    assert evaluation_calls == []

    expected_id = hashlib.sha256(
        f"{workspace_id}:refund-check:v2:trace-1".encode()
    ).hexdigest()

    def existing_score_post(path, body, *, include_status=False):
        if path.endswith("evaluators/active"):
            return {"items": [evaluator]}
        if path.endswith("sessions/evidence"):
            return {
                "session_id": "session-1", "agent_names": ["support-agent"],
                "scores": [{
                    "score_id": expected_id, "trace_id": "trace-1",
                    "config_id": "evaluator:refund-check:v2", "source": "evaluator",
                    "string_value": "pass",
                }],
                "spans": [{"trace_id": "trace-1", "start_time": "2026-10-09T12:00:00Z", "observation_type": "generation", "input": "hello"}],
            }
        raise AssertionError(path)

    monkeypatch.setattr(mcp_tools, "_thelake_post", existing_score_post)
    duplicate = mcp_tools.evaluate_session("support-agent", "Refund eligibility", "session-1")
    assert duplicate["status"] == "already_evaluated"
    assert "score_id" not in duplicate
    assert evaluation_calls == []


def test_evaluate_session_does_not_store_transient_judge_errors(monkeypatch):
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    evaluator = {
        "evaluator_id": "refund-check", "version": 1,
        "target_agent_name": "support-agent", "name": "Refund eligibility",
        "criteria": "Check eligibility before a refund.",
    }
    score_writes = []

    def fake_lake_post(path, _body, *, include_status=False):
        if path.endswith("evaluators/active"):
            return {"items": [evaluator]}
        if path.endswith("sessions/evidence"):
            return {
                "session_id": "session-1", "agent_names": ["support-agent"],
                "scores": [], "spans": [{"trace_id": "trace-1", "start_time": "2026-10-09T12:00:00Z"}],
            }
        if path.endswith("/scores"):
            score_writes.append(path)
            return ({"score_id": "score-1"}, 201)
        raise AssertionError(path)

    monkeypatch.setattr(mcp_tools, "_thelake_post", fake_lake_post)
    monkeypatch.setattr(mcp_tools, "_span_events", lambda _spans: ([{"kind": "user_message", "content": "refund"}], False, False))
    monkeypatch.setattr(mcp_tools, "run_evaluation", lambda _request: {
        "status": "error", "rationale": "Judge is temporarily unavailable.",
        "limitations": ["evaluation_request_or_provider_error"],
    })

    result = mcp_tools.evaluate_session("support-agent", "Refund eligibility", "session-1")

    assert result["status"] == "error"
    assert result["stored"] is False
    assert score_writes == []


def test_legacy_error_score_retry_is_deduplicated_and_notifiable(monkeypatch, tmp_path):
    import hashlib

    from thelake_lisa import mcp_tools

    workspace_id = "11111111-1111-4111-8111-111111111111"
    evaluator = {
        "evaluator_id": "refund-check", "version": 2,
        "target_agent_name": "support-agent", "name": "Refund eligibility",
        "criteria": "Check eligibility before a refund.", "slack_channel_id": "channel-1",
    }
    base_id = hashlib.sha256(
        f"{workspace_id}:refund-check:v2:trace-1".encode()
    ).hexdigest()
    legacy_error = {"score_id": base_id, "string_value": "error"}
    scores = [legacy_error]
    score_writes = []
    judge_calls = []

    def fake_lake_post(path, body, *, include_status=False):
        if path.endswith("evaluators/active"):
            return {"items": [evaluator]}
        if path.endswith("sessions/evidence"):
            return {
                "session_id": "session-1", "agent_names": ["support-agent"],
                "scores": list(scores),
                "spans": [{"trace_id": "trace-1", "start_time": "2026-10-09T12:00:00Z",
                           "observation_type": "generation", "input": "refund"}],
            }
        if path.endswith("/scores"):
            score_writes.append(body)
            scores.append({
                "score_id": body["score_id"], "trace_id": body["trace_id"],
                "config_id": body["config_id"], "source": body["source"],
                "string_value": body["string_value"], "comment": body["comment"],
            })
            result = {"score_id": body["score_id"]}
            return (result, 201) if include_status else result
        raise AssertionError(path)

    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(mcp_tools, "_thelake_post", fake_lake_post)
    monkeypatch.setattr(mcp_tools, "_span_events", lambda _spans: ([{"kind": "user_message", "content": "refund"}], False, False))
    monkeypatch.setattr(mcp_tools, "run_evaluation", lambda request: judge_calls.append(request) or {
        "status": "fail", "score": 0.2, "rationale": "Missing eligibility check.",
        "framework": "deepeval", "evidence": [], "limitations": [],
    })

    first = mcp_tools.evaluate_session("support-agent", "Refund eligibility", "session-1")
    second = mcp_tools.evaluate_session("support-agent", "Refund eligibility", "session-1")
    notified = []
    monkeypatch.setattr(mcp_tools, "_notify_failure", lambda *args: notified.append(args) or {"status": "sent"})
    notification = mcp_tools.notify_evaluation_failure("support-agent", "Refund eligibility", "session-1")

    assert first["status"] == "fail"
    assert "score_id" not in first
    assert second["status"] == "already_evaluated"
    assert "score_id" not in second
    assert len(judge_calls) == 1
    assert len(score_writes) == 1
    assert notification == {"status": "sent"}
    assert notified[0][1]["rationale"] == "Missing eligibility check."


def test_failure_notification_requires_a_stored_failure(monkeypatch):
    from thelake_lisa import mcp_tools

    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    evaluator = {"evaluator_id": "check-1", "version": 1, "name": "Test", "target_agent_name": "agent", "slack_channel_id": "channel"}
    monkeypatch.setattr(mcp_tools, "_active_evaluations", lambda: [evaluator])
    monkeypatch.setattr(mcp_tools, "_thelake_post", lambda *_args, **_kwargs: {
        "session_id": "session-1", "agent_names": ["agent"], "scores": [], "spans": [{"trace_id": "trace-1"}]
    })

    result = mcp_tools.notify_evaluation_failure("agent", "Test", "session-1")

    assert result == {"status": "not_a_stored_failure", "session_id": "session-1"}


def test_failure_notification_only_sends_after_lisa_verifies_stored_failure(monkeypatch):
    from hashlib import sha256
    from thelake_lisa import mcp_tools

    workspace_id = "11111111-1111-4111-8111-111111111111"
    evaluator = {
        "evaluator_id": "check-1", "version": 2, "name": "Policy check",
        "target_agent_name": "support-agent", "slack_channel_id": "channel-1",
    }
    score_id = sha256(f"{workspace_id}:check-1:v2:trace-1".encode()).hexdigest()
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setattr(mcp_tools, "_active_evaluations", lambda: [evaluator])
    monkeypatch.setattr(mcp_tools, "_thelake_post", lambda *_args, **_kwargs: {
        "session_id": "session-1", "agent_names": ["support-agent"],
        "scores": [{
            "score_id": score_id, "trace_id": "trace-1",
            "config_id": "evaluator:check-1:v2", "source": "evaluator",
            "string_value": "fail", "comment": "Missing eligibility check.",
        }],
        "spans": [{"trace_id": "trace-1"}],
    })
    sent = []
    monkeypatch.setattr(mcp_tools, "_notify_failure", lambda *args: sent.append(args) or {"status": "sent"})

    result = mcp_tools.notify_evaluation_failure("support-agent", "Policy check", "session-1")

    assert result == {"status": "sent"}
    assert sent[0][0] == evaluator
    assert sent[0][1]["rationale"] == "Missing eligibility check."
    assert sent[0][2] == "session-1"


def test_failure_notification_routes_and_deduplicates_legacy_check(monkeypatch, tmp_path):
    import json

    from thelake_lisa import mcp_tools

    workspace_id = "11111111-1111-4111-8111-111111111111"
    evaluator = {
        "evaluator_id": "legacy-check", "version": 3, "name": "Refund eligibility",
        "target_agent_name": "support-agent", "slack_channel_id": None,
        "slack_thread_ts": "thread-from-unbound-channel",
    }
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "test-bot-token")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "workspace-alerts")
    monkeypatch.setattr(mcp_tools, "_active_evaluations", lambda: [evaluator])
    monkeypatch.setattr(mcp_tools, "_thelake_post", lambda *_args, **_kwargs: {
        "session_id": "session-1", "agent_names": ["support-agent"],
        "spans": [{"trace_id": "trace-1"}],
        "scores": [{
            "score_id": mcp_tools._evaluation_score_id(evaluator, "trace-1"),
            "trace_id": "trace-1", "config_id": "evaluator:legacy-check:v3",
            "source": "evaluator",
            "string_value": "fail", "comment": "Missing eligibility check.",
        }],
    })
    requests = []

    def fake_urlopen(request, timeout):
        requests.append((json.loads(request.data), timeout))
        return io.BytesIO(json.dumps({"ok": True, "ts": "1710000000.000001"}).encode())

    monkeypatch.setattr(mcp_tools.urllib.request, "urlopen", fake_urlopen)

    result = mcp_tools.notify_evaluation_failure("support-agent", "Refund eligibility", "session-1")
    repeated_result = mcp_tools.notify_evaluation_failure("support-agent", "Refund eligibility", "session-1")

    assert result["status"] == "sent"
    assert requests[0][0]["channel"] == "workspace-alerts"
    assert "thread_ts" not in requests[0][0]
    assert "session-1" in requests[0][0]["text"]
    assert repeated_result == {"status": "already_sent", "session_id": "session-1"}
    assert len(requests) == 1
