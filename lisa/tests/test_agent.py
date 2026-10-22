from pathlib import Path
from uuid import UUID

import pytest

from lisa.backends import openhands as agent
from lisa.security import redact_text


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


def test_openhands_turn_uses_explicit_activity_resolution():
    from lisa.runtime import selected_chat_activity_ids

    assert selected_chat_activity_ids([
        {"kind": "evidence", "tool_name": "resolve_chat_activity",
         "detail": '{"disposition":"continue","activity_id":"activity-1"}'},
    ]) == ["activity-1"]
    assert selected_chat_activity_ids([
        {"kind": "tool", "tool_name": "update_activity",
         "arguments": {"activity_id": "activity-1"}},
    ]) == []


def test_redacts_credentials_in_user_messages_before_they_are_persisted():
    text = (
        'Please inspect this: api_key="message-secret-value" and Bearer abcdefghijklmnopqrstuvwxyz'
    )

    redacted = redact_text(text)

    assert "message-secret-value" not in redacted
    assert "abcdefghijklmnopqrstuvwxyz" not in redacted
    assert "api_key" in redacted
    assert "[REDACTED]" in redacted


def test_redacts_bearer_values_inside_authorization_fields():
    redacted = redact_text("authorization: Bearer telemetry-secret-value")

    assert "telemetry-secret-value" not in redacted
    assert "authorization: [REDACTED]" == redacted


def test_redacts_credentials_in_json_chat_messages():
    redacted = redact_text('{"api_key": "arbitrary-json-secret"}')

    assert "arbitrary-json-secret" not in redacted
    assert redacted == '{"api_key": "[REDACTED]"}'


def test_redacts_credentials_in_composite_json_keys():
    text = '{"AWS_SECRET_ACCESS_KEY": "aws-composite-secret", "OPENAI_API_KEY": "openai-composite-secret"}'

    redacted = redact_text(text)

    assert "aws-composite-secret" not in redacted
    assert "openai-composite-secret" not in redacted
    assert '"AWS_SECRET_ACCESS_KEY": "[REDACTED]"' in redacted
    assert '"OPENAI_API_KEY": "[REDACTED]"' in redacted


def test_redacts_credentials_in_camel_case_json_keys():
    redacted = redact_text(
        '{"clientSecret": "camel-case-secret", "accessToken": "camel-token-secret"}'
    )

    assert "camel-case-secret" not in redacted
    assert "camel-token-secret" not in redacted
    assert '"clientSecret": "[REDACTED]"' in redacted
    assert '"accessToken": "[REDACTED]"' in redacted


def test_redacts_json_credentials_embedded_in_prose():
    redacted = redact_text('Inspect this config: {"clientSecret": "prose-json-secret"}.')

    assert "prose-json-secret" not in redacted
    assert '"clientSecret": "[REDACTED]"' in redacted


@pytest.mark.parametrize("use_configured_skill_dir", [True, False])
def test_lisa_agent_uses_openhands_skills_and_mcp_config(
    monkeypatch, tmp_path, use_configured_skill_dir
):
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake:8090")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "test-token")
    monkeypatch.setenv("GOOGLE_API_KEY", "judge-test-key")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "slack-test-token")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "channel-test")
    monkeypatch.setenv("LISA_SLACK_ACTIVITY_CHANNEL_ID", "activity-channel-test")
    monkeypatch.setenv("LISA_SLACK_ACTIVITY_THREAD_TS", "12345.67")
    monkeypatch.setenv("LISA_DATABASE_READ_URL", "postgresql://reader:test@postgres/lisa")
    configured_skill_dir = (
        Path(__file__).parents[1] / "skills"
        if use_configured_skill_dir
        else tmp_path / "missing-skills"
    )
    monkeypatch.setenv("LISA_SKILL_DIR", str(configured_skill_dir))
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
    assert lisa_mcp.args == ["-m", "lisa.mcp_app"]
    assert (
        lisa_mcp.env["LISA_WORKSPACE_ID"].get_secret_value()
        == "11111111-1111-4111-8111-111111111111"
    )
    assert lisa_mcp.env["GOOGLE_API_KEY"].get_secret_value() == "judge-test-key"
    assert lisa_mcp.env["LISA_SLACK_BOT_TOKEN"].get_secret_value() == "slack-test-token"
    assert lisa_mcp.env["LISA_SLACK_CHANNEL_ID"].get_secret_value() == "channel-test"
    assert lisa_mcp.env["LISA_SLACK_ACTIVITY_CHANNEL_ID"].get_secret_value() == "activity-channel-test"
    assert lisa_mcp.env["LISA_SLACK_ACTIVITY_THREAD_TS"].get_secret_value() == "12345.67"
    assert "lisa.readable_activities" in captured["system_prompt"]
    assert "readable_activity_events: activity_id, seq, event_type" in captured["system_prompt"]
    assert "reference_type, reference_id, created_at" in captured["system_prompt"]
    skill_names = {skill.name for skill in captured["agent_context"].skills}
    assert {
        "learn-agent-policy",
        "review-agent-policy",
        "investigate-evaluator-failure",
        "manage-lisa-activities",
    } <= skill_names
    assert "For each distinct user request" in captured["system_prompt"]
    assert "Never resolve the same request twice in one turn" in captured["system_prompt"]
    assert "Keep the exact activity ID" in captured["system_prompt"]
    assert "Approve policy: <policy_id>" in captured["system_prompt"]
    assert any(
        "Never resolve the same request twice in one turn" in skill.content
        for skill in captured["agent_context"].skills
        if skill.name == "manage-lisa-activities"
    )


def test_lisa_mcp_exposes_policy_session_and_evaluator_tools(monkeypatch, lisa_test_context):
    import asyncio

    from fastmcp import Client

    from lisa.bootstrap import build_services
    from lisa.controllers.mcp import create_server

    services = build_services(
        lisa_test_context(api_url="http://thelake:8090", api_token="test-token")
    )
    server = create_server(services)

    async def list_tools():
        async with Client(server) as client:
            return {tool.name: tool for tool in await client.list_tools()}

    tools = asyncio.run(list_tools())

    assert {
        "list_policies",
        "read_policy",
        "write_policy",
        "approve_policy",
        "delete_policy",
        "list_activity_followups",
        "search_sessions",
        "evaluate_session",
        "notify_evaluation_failure",
    } <= tools.keys()
    assert "openhands" not in Path(create_server.__code__.co_filename).read_text(encoding="utf-8").lower()


def test_call_recording_setting_and_conversation_link_reach_mcp(monkeypatch, tmp_path):
    monkeypatch.setenv("GEMINI_API_KEY", "synthetic-key")
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    captured = {}
    monkeypatch.setattr(agent, "LLM", lambda **_: object())
    monkeypatch.setattr(agent, "Agent", lambda **kwargs: captured.update(kwargs) or object())
    monkeypatch.setattr(agent, "Conversation", lambda **_: object())
    conversation_id = "be3c93b1-d225-4ce7-8dff-06ac2924289f"
    agent.build_conversation(
        conversation_id, str(tmp_path), workspace_id="11111111-1111-4111-8111-111111111111"
    )
    environment = captured["mcp_config"]["lisa"].env
    assert environment["LISA_LLM_TRACE_ENABLED"].get_secret_value() == "1"
    assert environment["LISA_CONVERSATION_ID"].get_secret_value() == conversation_id
