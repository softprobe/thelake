from pathlib import Path
from uuid import UUID

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


def test_lisa_agent_uses_openhands_skills_and_mcp_config(monkeypatch, tmp_path):
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake:8090")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "test-token")
    monkeypatch.setenv("GOOGLE_API_KEY", "judge-test-key")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "slack-test-token")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "channel-test")
    monkeypatch.setenv("LISA_DATABASE_READ_URL", "postgresql://reader:test@postgres/lisa")
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
    assert lisa_mcp.args == ["-m", "lisa.mcp_app"]
    assert (
        lisa_mcp.env["LISA_WORKSPACE_ID"].get_secret_value()
        == "11111111-1111-4111-8111-111111111111"
    )
    assert lisa_mcp.env["GOOGLE_API_KEY"].get_secret_value() == "judge-test-key"
    assert lisa_mcp.env["LISA_SLACK_BOT_TOKEN"].get_secret_value() == "slack-test-token"
    assert lisa_mcp.env["LISA_SLACK_CHANNEL_ID"].get_secret_value() == "channel-test"
    assert "lisa.readable_work_items" in captured["system_prompt"]
    assert "readable_work_events: work_item_id, seq, event_type" in captured["system_prompt"]
    skill_names = {skill.name for skill in captured["agent_context"].skills}
    assert {
        "learn-agent-policy",
        "review-agent-policy",
        "investigate-evaluator-failure",
    } <= skill_names


def test_lisa_mcp_exposes_policy_session_and_evaluator_tools(tmp_path, monkeypatch):
    import asyncio

    from fastmcp import Client

    from lisa.bootstrap import build_services
    from lisa.context import WorkspaceContext
    from lisa.controllers.mcp import create_server

    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake:8090")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "test-token")

    services = build_services(WorkspaceContext.from_environment())
    server = create_server(services)

    async def list_tools():
        async with Client(server) as client:
            return {tool.name: tool for tool in await client.list_tools()}

    tools = asyncio.run(list_tools())

    assert {
        "list_policies",
        "read_policy",
        "update_policy",
        "search_sessions",
        "evaluate_session",
        "notify_evaluation_failure",
    } <= tools.keys()
    assert "create_evaluation_draft" not in tools
    assert "activate_evaluation" not in tools
    assert "sample_agent_sessions" not in tools
    assert "select_policies_for_session" not in tools
    assert "confirmed_rule_title" not in tools["update_policy"].inputSchema["properties"]
    assert "openhands" not in Path(create_server.__code__.co_filename).read_text(encoding="utf-8").lower()
