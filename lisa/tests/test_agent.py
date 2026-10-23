from pathlib import Path
from types import SimpleNamespace
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

    monkeypatch.setattr(agent, "LocalConversation", conversation_factory)
    conversation_id = "be3c93b1-d225-4ce7-8dff-06ac2924289f"

    agent.build_conversation(
        conversation_id,
        str(tmp_path),
        workspace_id="11111111-1111-4111-8111-111111111111",
    )

    assert captured["conversation_id"] == UUID(conversation_id)
    assert captured["visualizer"] is None


def test_agent_prompt_does_not_cache_per_turn_owner_authorization(monkeypatch, tmp_path):
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setenv("LISA_OWNER_USER_IDS", "owner-1")
    captured = []

    monkeypatch.setattr(agent, "LLM", lambda **kwargs: object())
    monkeypatch.setattr(agent, "Agent", lambda **kwargs: captured.append(kwargs) or object())
    monkeypatch.setattr(agent, "LocalConversation", lambda **kwargs: object())

    for actor_id in ("owner-1", "member-2"):
        agent.build_conversation(
            str(UUID(int=len(captured) + 1)),
            str(tmp_path / str(len(captured))),
            workspace_id="11111111-1111-4111-8111-111111111111",
            actor_id=actor_id,
        )

    assert captured[0]["system_prompt"] == captured[1]["system_prompt"]
    assert "Trusted runtime authorization for this turn" not in captured[0]["system_prompt"]
    assert "Owner-only tools" in captured[0]["system_prompt"]


def test_backend_resets_checkpoint_tools_before_sending_turn(monkeypatch, tmp_path):
    calls = []

    class Condenser:
        def prepare_for_turn(self, events):
            assert events == []
            calls.append("reset")

    class Conversation:
        state = SimpleNamespace(events=[])
        agent = SimpleNamespace(condenser=Condenser())

        def send_message(self, _text):
            assert calls == ["reset"]
            calls.append("send")

        def run(self):
            self.state.events.append(SimpleNamespace(id="reply"))

        def close(self):
            calls.append("close")

    monkeypatch.setattr(agent, "install_call_logger", lambda: None)
    monkeypatch.setattr(agent, "build_conversation", lambda *_args, **_kwargs: Conversation())
    monkeypatch.setattr(
        agent,
        "conversation_content",
        lambda _conversation: ([{"id": "reply", "role": "assistant", "text": "Done."}], []),
    )
    monkeypatch.delenv("LISA_DATABASE_URL", raising=False)

    result = agent.OpenHandsBackend().run_turn(
        conversation_id="22222222-2222-4222-8222-222222222222",
        directory=str(tmp_path),
        workspace_id="11111111-1111-4111-8111-111111111111",
        text="Continue.",
    )

    assert calls == ["reset", "send", "close"]
    assert result.messages[0]["text"] == "Done."


def test_openhands_stdio_loads_and_exposes_namespace_tools(
    monkeypatch, tmp_path, lisa_test_context
):
    from lisa.conversation_context import event_prefix_fingerprint
    from lisa.repositories.conversation_context import checkpoint_request_id
    from lisa.repositories.conversations import FileConversationRepository

    conversation_id = "33333333-3333-4333-8333-333333333333"
    context = lisa_test_context(conversation_id=conversation_id, data_dir=tmp_path / "data")
    directory = FileConversationRepository(context.data_dir).conversation_directory(
        context.workspace_id(), conversation_id
    )
    directory.mkdir(parents=True)
    for name, value in {
        "GEMINI_API_KEY": "test-key",
        "LISA_WORKSPACE_ID": context.workspace_id(),
        "LISA_DATA_DIR": str(context.data_dir),
        "LISA_DATABASE_URL": context.database_url,
        "LISA_DATABASE_READ_URL": context.database_read_url,
        "LISA_OWNER_USER_IDS": "owner-1",
        "LISA_AGENT_WORKSPACE_ROOT": str(tmp_path / "agent-workspace"),
    }.items():
        monkeypatch.setenv(name, value)

    conversation = agent.build_conversation(
        conversation_id,
        str(directory),
        workspace_id=context.workspace_id(),
        actor_id="owner-1",
    )
    try:
        conversation.send_message("Load a tool namespace.")
        tools = conversation.agent.tools_map
        assert {tool.name.rsplit(".", 1)[-1] for tool in tools.values()} >= {
            "discover_tool_namespaces",
            "load_tool_namespace",
        }
        loader = next(tool for tool in tools.values() if tool.name.endswith("load_tool_namespace"))

        def load_namespace(namespace):
            observation = loader.executor(
                loader.action_from_arguments({"namespace": namespace}), conversation
            )
            assert not observation.is_error, observation

        load_namespace("policies")
        assert any(
            tool.name.endswith("list_policies") for tool in conversation.agent.tools_map.values()
        )
        assert any(
            tool.name.endswith("write_policy") for tool in conversation.agent.tools_map.values()
        )

        load_namespace("conversation")
        todo_writer = next(
            tool
            for tool in conversation.agent.tools_map.values()
            if tool.name.endswith("todo_write")
        )
        todo_observation = todo_writer.executor(
            todo_writer.action_from_arguments(
                {"items": [{"id": "review", "text": "Review P1.1", "status": "open"}]}
            ),
            conversation,
        )
        assert not todo_observation.is_error, todo_observation
        context_repository = FileConversationRepository(context.data_dir).context_repository(
            directory
        )
        assert context_repository.read_todo() == [
            {"id": "review", "status": "open", "text": "Review P1.1"}
        ]

        events = list(conversation.state.events)
        configuration = conversation.agent.condenser.configuration_fingerprint
        prefix_fingerprint = event_prefix_fingerprint(events)
        request_id = checkpoint_request_id(configuration, prefix_fingerprint)
        context_repository.publish_checkpoint(
            request_id,
            {
                "summary": "The user asked Lisa to review P1.1.",
                "prefix_event_ids": [event.id for event in events],
                "prefix_fingerprint": prefix_fingerprint,
                "config_fingerprint": configuration,
                "captured_at": "2026-10-22T00:00:00+00:00",
            },
        )
    finally:
        conversation.close()

    resumed = agent.build_conversation(
        conversation_id,
        str(directory),
        workspace_id=context.workspace_id(),
        actor_id="owner-1",
    )
    try:
        assert resumed.agent.condenser.prepare_for_turn(resumed.state.events)
        assert context_repository.read_todo() == [
            {"id": "review", "status": "open", "text": "Review P1.1"}
        ]
        resumed.send_message("Continue the checklist.")
        names = {tool.name.rsplit(".", 1)[-1] for tool in resumed.agent.tools_map.values()}
        assert {"discover_tool_namespaces", "load_tool_namespace"} <= names
        assert names <= {
            "discover_tool_namespaces",
            "load_tool_namespace",
            "invoke_skill",
        }

        loader = next(
            tool
            for tool in resumed.agent.tools_map.values()
            if tool.name.endswith("load_tool_namespace")
        )
        observation = loader.executor(
            loader.action_from_arguments({"namespace": "conversation"}), resumed
        )
        assert not observation.is_error, observation
        assert any(tool.name.endswith("todo_read") for tool in resumed.agent.tools_map.values())
    finally:
        resumed.close()


def test_provider_endpoint_fingerprint_excludes_credentials_and_query():
    assert (
        agent._provider_endpoint_fingerprint(
            "https://user:secret@example.test/v1?api_key=hidden#fragment"
        )
        == "https://example.test/v1"
    )


def test_openhands_turn_uses_explicit_activity_resolution():
    from lisa.runtime import selected_chat_activity_ids

    assert selected_chat_activity_ids(
        [
            {
                "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": '{"disposition":"continue","activity_id":"activity-1"}',
            },
        ]
    ) == ["activity-1"]
    assert (
        selected_chat_activity_ids(
            [
                {
                    "kind": "tool",
                    "tool_name": "update_activity",
                    "arguments": {"activity_id": "activity-1"},
                },
            ]
        )
        == []
    )


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
    monkeypatch.setenv("LISA_AGENT_WORKSPACE_ROOT", "/tmp/lisa-agent-workspace")
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
    monkeypatch.setattr(agent, "LocalConversation", lambda **kwargs: object())

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
    assert (
        lisa_mcp.env["LISA_SLACK_ACTIVITY_CHANNEL_ID"].get_secret_value() == "activity-channel-test"
    )
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
    assert "call approve_policy with" in captured["system_prompt"]
    assert "LISA_CONFIRMED_POLICY_ID" not in lisa_mcp.env
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

    services = build_services(lisa_test_context())
    server = create_server(services)

    async def list_tools():
        async with Client(server) as client:
            return {tool.name: tool for tool in await client.list_tools()}

    async def loaded_tools():
        async with Client(server) as client:
            for namespace in ("policies", "sessions", "evaluation"):
                await client.call_tool("load_tool_namespace", {"namespace": namespace})
            return {tool.name: tool for tool in await client.list_tools()}

    tools = asyncio.run(loaded_tools())

    assert {
        "list_policies",
        "read_policy",
        "write_policy",
        "approve_policy",
        "delete_policy",
        "search_sessions",
        "evaluate_session",
        "notify_evaluation_failure",
    } <= tools.keys()
    assert (
        "openhands"
        not in Path(create_server.__code__.co_filename).read_text(encoding="utf-8").lower()
    )


def test_call_recording_setting_and_conversation_link_reach_mcp(monkeypatch, tmp_path):
    monkeypatch.setenv("GEMINI_API_KEY", "synthetic-key")
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    captured = {}
    monkeypatch.setattr(agent, "LLM", lambda **_: object())
    monkeypatch.setattr(agent, "Agent", lambda **kwargs: captured.update(kwargs) or object())
    monkeypatch.setattr(agent, "LocalConversation", lambda **_: object())
    conversation_id = "be3c93b1-d225-4ce7-8dff-06ac2924289f"
    agent.build_conversation(
        conversation_id, str(tmp_path), workspace_id="11111111-1111-4111-8111-111111111111"
    )
    environment = captured["mcp_config"]["lisa"].env
    assert environment["LISA_LLM_TRACE_ENABLED"].get_secret_value() == "1"
    assert environment["LISA_CONVERSATION_ID"].get_secret_value() == conversation_id
