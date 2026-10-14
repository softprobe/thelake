import asyncio
import json
from pathlib import Path

from fastmcp import Client

from thelake_lisa.context import WorkspaceContext
from thelake_lisa.mcp_tools import create_server
from thelake_lisa.services import build_services


def test_mcp_public_policy_contract(tmp_path):
    server = create_server(
        build_services(
            WorkspaceContext(
                workspace="11111111-1111-4111-8111-111111111111",
                data_dir=tmp_path,
            )
        )
    )

    async def contract():
        async with Client(server) as client:
            tools = await client.list_tools()
        return sorted(
            [
                {
                    key: getattr(tool, key)
                    for key in ("name", "description", "inputSchema", "outputSchema")
                }
                for tool in tools
            ],
            key=lambda tool: tool["name"],
        )

    expected = json.loads((Path(__file__).parent / "fixtures/mcp_tool_contract.json").read_text())
    assert asyncio.run(contract()) == expected


def test_mcp_delegates_to_scoped_services_and_redacts_results(monkeypatch, tmp_path):
    services = build_services(
        WorkspaceContext(
            workspace="11111111-1111-4111-8111-111111111111",
            data_dir=tmp_path,
        )
    )
    calls = []

    def post(path, body):
        calls.append((path, body))
        return {
            "items": [
                {
                    "session_id": "session",
                    "start_time": "2026-10-12T00:00:00Z",
                    "end_time": None,
                    "trace_count": 1,
                    "span_count": 3,
                    "error_count": 0,
                    "input_tokens": 10,
                    "output_tokens": 2,
                    "total_tokens": 12,
                    "total_cost": None,
                    "agent_name": "agent",
                    "user_ids": [],
                    "models": ["test-model"],
                }
            ],
            "next_cursor": None,
            "cursor_supported": True,
        }

    monkeypatch.setattr(services.client, "post", post)

    async def call():
        async with Client(create_server(services)) as client:
            return await client.call_tool(
                "search_sessions",
                {
                    "from": "2026-10-11T00:00:00Z",
                    "to": "2026-10-12T00:00:00Z",
                    "agent_name": "agent",
                    "has_errors": False,
                    "limit": 10,
                },
            )

    result = asyncio.run(call())
    assert not result.is_error
    assert calls[0][0] == "/v1/sessions/search"
    assert calls[0][1] == {
        "from": "2026-10-11T00:00:00Z",
        "to": "2026-10-12T00:00:00Z",
        "has_errors": False,
        "user_id": None,
        "model_name": None,
        "agent_name": "agent",
        "roots_only": True,
        "order_by": "start_time",
        "order": "desc",
        "limit": 10,
        "cursor": None,
    }
    assert json.loads(result.content[0].text) == {
        "items": [
            {
                "session_id": "session",
                "start_time": "2026-10-12T00:00:00Z",
                "end_time": None,
                "trace_count": 1,
                "span_count": 3,
                "error_count": 0,
                "input_tokens": 10,
                "output_tokens": 2,
                "total_tokens": 12,
                "total_cost": None,
                "agent_name": "agent",
                "user_ids": [],
                "models": ["test-model"],
            }
        ],
        "next_cursor": None,
        "cursor_supported": True,
    }


def test_mcp_get_session_returns_the_evaluators_compact_trajectory(monkeypatch, tmp_path):
    services = build_services(
        WorkspaceContext(
            workspace="11111111-1111-4111-8111-111111111111",
            data_dir=tmp_path,
        )
    )
    trajectory = {
        "session_id": "session",
        "agent_names": ["agent"],
        "trace_ids": ["trace"],
        "events": [
            {
                "sequence": 0,
                "span_id": "span",
                "timestamp": "2026-10-12T00:00:00Z",
                "kind": "user_message",
                "content": "Refund this ticket",
            }
        ],
        "complete": True,
        "tool_events_captured": False,
        "tool_order_certain": True,
        "message_order_certain": True,
        "limitations": [],
    }
    monkeypatch.setattr(services.evaluations, "get_session_trajectory", lambda _id: trajectory)

    async def call():
        async with Client(create_server(services)) as client:
            return await client.call_tool("get_session", {"session_id": "session"})

    result = asyncio.run(call())
    assert not result.is_error
    assert json.loads(result.content[0].text) == trajectory


def test_context_captures_workspace_and_approvals_once(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Rule")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "private-lake-token")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "private-slack-token")
    context = WorkspaceContext.from_environment()
    monkeypatch.setenv("LISA_WORKSPACE_ID", "22222222-2222-4222-8222-222222222222")
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_TITLE")
    services = build_services(context)
    services.policies.update_policy("## Rule\n\nConfirmed rule", "")
    assert context.workspace_id() == "11111111-1111-4111-8111-111111111111"
    assert services.policies.read_policy() == "## Rule\n\nConfirmed rule"
    assert "private-lake-token" not in repr(context)
    assert "private-slack-token" not in repr(context)


def test_stdio_entrypoint_reads_and_updates_workspace_policy(tmp_path):
    import sys

    config = {
        "mcpServers": {
            "lisa": {
                "command": sys.executable,
                "args": ["-m", "thelake_lisa.mcp_tools"],
                "env": {
                    "LISA_WORKSPACE_ID": "11111111-1111-4111-8111-111111111111",
                    "LISA_DATA_DIR": str(tmp_path),
                    "LISA_TURN_SOURCE": "web",
                    "LISA_CONFIRMED_POLICY_TITLE": "Rule",
                },
            }
        }
    }

    async def roundtrip():
        async with Client(config) as client:
            first = await client.call_tool("read_policy", {})
            assert first.data == "No policy exists yet."
            saved = await client.call_tool(
                "update_policy",
                {
                    "content": "## Rule\napproved: yes\n\nApproved policy",
                    "expected_content": first.data,
                },
            )
            assert not saved.is_error
            read = await client.call_tool("read_policy", {})
            assert read.data == "## Rule\napproved: yes\n\nApproved policy"
            rules = await client.call_tool("list_approved_policies", {"target_agent_name": "agent"})
            assert rules.data[0]["name"] == "Rule"

    asyncio.run(roundtrip())
