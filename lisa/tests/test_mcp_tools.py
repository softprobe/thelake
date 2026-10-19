import asyncio
import json
from dataclasses import replace
from pathlib import Path

import pytest
from fastmcp import Client
from policy_cases import read_policy

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.mcp_app import create_server


def test_mcp_public_policy_contract(lisa_test_context):
    server = create_server(
        build_services(lisa_test_context())
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


def test_mcp_activity_tools_find_and_complete_owned_work(lisa_test_context):
    services = build_services(lisa_test_context())
    assert services.activity_store is not None

    async def workflow():
        async with Client(create_server(services)) as client:
            created = await client.call_tool(
                "create_activity",
                {"title": "Review session", "activity_type": "task_running", "source": "chat"},
            )
            activity_id = created.data["activity_id"]
            recent = await client.call_tool("list_recent_activities", {"limit": 10})
            assert any(item["activity_key"] == activity_id for item in recent.data)
            updated = await client.call_tool(
                "update_activity",
                {
                    "activity_id": activity_id,
                    "event_type": "progress",
                    "event_title": "Session reviewed",
                },
            )
            completed = await client.call_tool(
                "complete_activity",
                {"activity_id": activity_id, "status": "success", "summary": "No issue found."},
            )
            thread = await client.call_tool("get_activity_thread", {"activity_id": activity_id})
            return updated, completed, thread

    updated, completed, thread = asyncio.run(workflow())
    assert not updated.is_error
    assert completed.data["completed"] is True
    assert thread.data["thread"]["status"] == "success"
    assert thread.data["thread"]["finish_message"] == "No issue found."


def test_policy_proposal_can_be_created_in_main_and_confirmed_in_slack(lisa_test_context):
    context = lisa_test_context(
        turn_source="web", conversation_id="main-chat", actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)

    async def create():
        async with Client(create_server(services)) as client:
            activity = await client.call_tool("create_activity", {
                "title": "Update refund policy", "activity_type": "policy", "source": "web",
            })
            result = await client.call_tool("create_policy_proposal", {
                "activity_id": activity.data["activity_id"],
                "scope": "workspace",
                "expected_content": "No policy exists yet.",
                "proposed_content": "## Refund eligibility\napproved: no\n\nCheck eligibility.",
            })
            rejected = await client.call_tool("create_policy_proposal", {
                "activity_id": activity.data["activity_id"],
                "scope": "workspace",
                "expected_content": "No policy exists yet.",
                "proposed_content": "## Chargeback review\napproved: no\n\nReview chargebacks.",
            })
            return result.data["proposal_id"], rejected.data["proposal_id"], activity.data["activity_id"]

    proposal_id, rejected_id, activity_id = asyncio.run(create())
    slack_services = build_services(replace(
        context, turn_source="slack", conversation_id="slack-thread",
        confirmed_policy_proposal_id=proposal_id,
    ))

    async def apply():
        async with Client(create_server(slack_services)) as client:
            return await client.call_tool("apply_policy_proposal", {"proposal_id": proposal_id})

    result = asyncio.run(apply())
    assert not result.is_error
    assert result.data["status"] == "applied"

    outsider_services = build_services(replace(
        context, turn_source="slack", actor_id="channel-member",
        confirmed_policy_proposal_id=rejected_id,
    ))
    async def unauthorized_reject():
        async with Client(create_server(outsider_services)) as client:
            return await client.call_tool("reject_policy_proposal", {"proposal_id": rejected_id})
    with pytest.raises(Exception, match="authorized Lisa owner identity"):
        asyncio.run(unauthorized_reject())

    slack_services = build_services(replace(
        context, turn_source="slack", conversation_id="slack-thread",
        rejected_policy_proposal_id=rejected_id,
    ))

    async def reject():
        async with Client(create_server(slack_services)) as client:
            return await client.call_tool("reject_policy_proposal", {"proposal_id": rejected_id})

    rejected = asyncio.run(reject())
    assert not rejected.is_error
    assert rejected.data["status"] == "rejected"
    assert any(
        record.name == "Refund eligibility"
        for record in slack_services.policy_service.read_policy_records()
    )
    assert slack_services.activity_store.get_activity_thread(activity_id)["actions"][-1]["title"] == "Policy proposal rejected"


def test_mcp_delegates_to_scoped_services_and_redacts_results(monkeypatch, lisa_test_context):
    services = build_services(lisa_test_context())
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


def test_mcp_get_session_returns_the_evaluators_compact_trajectory(monkeypatch, lisa_test_context):
    services = build_services(lisa_test_context())
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
    monkeypatch.setattr(services.sessions, "get_trajectory", lambda _id: trajectory)

    async def call():
        async with Client(create_server(services)) as client:
            return await client.call_tool("get_session", {"session_id": "session"})

    result = asyncio.run(call())
    assert not result.is_error
    assert json.loads(result.content[0].text) == trajectory


def test_context_captures_workspace_and_approvals_once(monkeypatch, lisa_test_database):
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Rule")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "private-lake-token")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "private-slack-token")
    database = lisa_test_database()
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    context = WorkspaceContext.from_environment()
    monkeypatch.setenv("LISA_WORKSPACE_ID", "22222222-2222-4222-8222-222222222222")
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_TITLE")
    services = build_services(context)
    services.policy_service.update_policy("## Rule\n\nConfirmed rule", "")
    assert context.workspace_id() == "11111111-1111-4111-8111-111111111111"
    assert "## P1.1 | Rule\napproved: no\n\nConfirmed rule" in read_policy(services)
    assert "private-lake-token" not in repr(context)
    assert "private-slack-token" not in repr(context)


def test_stdio_entrypoint_reads_and_applies_workspace_policy_proposal(tmp_path, lisa_test_context):
    import sys

    context = lisa_test_context(data_dir=tmp_path)
    context = replace(
        context, actor_id="owner-1", owner_user_ids=frozenset({"owner-1"}),
    )
    activity_store = build_services(context).activity_store
    activity = activity_store.create_activity(title="Update rule", source="web")
    proposal_id = activity_store.create_policy_proposal(
        activity_id=activity.id, scope="workspace", target_agent_name="",
        policy_name="Rule", expected_content="No policy exists yet.",
        proposed_content="## Rule\napproved: yes\n\nApproved policy",
        created_by_conversation="main-chat",
    )

    config = {
        "mcpServers": {
            "lisa": {
                "command": sys.executable,
                "args": ["-m", "lisa.mcp_app"],
                "env": {
                    "LISA_WORKSPACE_ID": context.workspace,
                    "LISA_DATA_DIR": str(context.data_dir),
                    "LISA_DATABASE_URL": context.database_url,
                    "LISA_DATABASE_READ_URL": context.database_read_url,
                    "LISA_TURN_SOURCE": "web",
                    "LISA_ACTOR_ID": "owner-1",
                    "LISA_OWNER_USER_IDS": "owner-1",
                        "LISA_CONFIRMED_POLICY_PROPOSAL_ID": proposal_id,
                        "LISA_CONVERSATION_ID": "main-chat",
                },
            }
        }
    }

    async def roundtrip():
        async with Client(config) as client:
            first = await client.call_tool("read_policy", {})
            assert first.data == "No policy exists yet."
            saved = await client.call_tool("apply_policy_proposal", {"proposal_id": proposal_id})
            assert not saved.is_error
            read = await client.call_tool("read_policy", {})
            assert read.data == (
                "# Workspace policies\n\n## P1.1 | Rule\napproved: yes\n\nApproved policy\n"
            )
            tools = {tool.name: tool for tool in await client.list_tools()}
            assert tools["evaluate_session"].inputSchema["required"] == [
                "target_agent_name",
                "session_id",
            ]
            assert "select_policies_for_session" not in tools
            assert "list_approved_policies" not in tools

    asyncio.run(roundtrip())


def test_lisa_database_tool_and_schema_resource_are_exposed(lisa_test_context):
    services = build_services(lisa_test_context())

    async def inspect_server():
        async with Client(create_server(services)) as client:
            tools = await client.list_tools()
            assert any(tool.name == "lisa_db" for tool in tools)
            resource = await client.read_resource("lisa://database/schema")
            assert "lisa.readable_activities" in str(resource)

    asyncio.run(inspect_server())


def test_evaluate_session_recovers_pending_scores_before_policy_or_telemetry_lookup(
    monkeypatch, lisa_test_context
):
    from types import SimpleNamespace

    services = build_services(lisa_test_context())
    services.evaluations.activities = SimpleNamespace(
        find_pending_evaluations_for_session=lambda agent, session: (
            [("activity", "outbox")] if (agent, session) == ("agent", "session") else []
        )
    )
    monkeypatch.setattr(
        services.evaluations,
        "_recover_pending_score",
        lambda activity, outbox: {"status": "pass", "activity_id": activity},
    )
    monkeypatch.setattr(
        services.client,
        "get",
        lambda _path: pytest.fail("Recovery must not fetch live telemetry"),
    )
    monkeypatch.setattr(
        services.evaluations.policy_triage,
        "triage",
        lambda *_args: pytest.fail("Recovery must not run policy selection"),
    )

    async def call():
        async with Client(create_server(services)) as client:
            return await client.call_tool(
                "evaluate_session",
                {"target_agent_name": "agent", "session_id": "session"},
            )

    result = asyncio.run(call())
    assert not result.is_error
    assert result.data["status"] == "recovered_pending_evaluations"
    assert result.data["evaluations"] == [{"status": "pass", "activity_id": "activity"}]
