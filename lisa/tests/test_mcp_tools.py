from benchmark.policy_seed import seed_policy_document

import asyncio
import json
import re
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


def test_policy_lifecycle_uses_only_human_readable_policy_id(lisa_test_context):
    context = lisa_test_context(
        turn_source="web", actor_id="owner-1", owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)

    async def create_and_find():
        async with Client(create_server(services)) as client:
            tools = {tool.name for tool in await client.list_tools()}
            assert {"write_policy", "approve_policy", "list_policies", "read_policy"} <= tools
            assert not any("proposal" in name for name in tools)
            created = await client.call_tool("write_policy", {
                "scope": "agent",
                "agent_name": "billing-agent",
                "name": "Billing changes",
                "criteria": "Record the reason before changing billing details.",
            })
            assert not created.is_error
            policy_id = created.data["policy_id"]
            assert re.fullmatch(r"P[1-9][0-9]*(?:\.[1-9][0-9]*){1,2}", policy_id)
            assert created.data["approved"] is False
            found = await client.call_tool("read_policy", {
                "scope": "agent", "agent_name": "billing-agent",
            })
            assert policy_id in found.data
            assert "approved: no" in found.data
            assert "Record the reason before changing billing details." in found.data
            return policy_id

    policy_id = asyncio.run(create_and_find())
    approved_services = build_services(replace(context, confirmed_policy_id=policy_id))

    async def approve():
        async with Client(create_server(approved_services)) as client:
            return await client.call_tool("approve_policy", {"policy_id": policy_id})

    approved = asyncio.run(approve())
    assert not approved.is_error
    assert approved.data["policy_id"] == policy_id
    assert approved.data["approved"] is True
    assert approved_services.policy_service.read_policy_records(
        "agent", "billing-agent",
    )[0].approved is True

    async def update():
        async with Client(create_server(services)) as client:
            return await client.call_tool("write_policy", {
                "scope": "agent",
                "agent_name": "billing-agent",
                "policy_id": policy_id,
                "name": "Billing changes",
                "criteria": "Record and verify the reason before changing billing details.",
            })

    updated = asyncio.run(update())
    assert not updated.is_error
    revision_id = updated.data["policy_id"]
    assert revision_id == f"{policy_id}.1"
    assert updated.data["approved"] is False
    rows = services.policy_service.read_policy_records("agent", "billing-agent")
    assert [(row.id, row.approved) for row in rows] == [(policy_id, True), (revision_id, False)]
    assert [row.id for row in services.policy_service.approved_rules("billing-agent")] == [policy_id]
    revision_services = build_services(replace(context, confirmed_policy_id=revision_id))
    async def approve_revision():
        async with Client(create_server(revision_services)) as client:
            return await client.call_tool("approve_policy", {"policy_id": revision_id})
    revised = asyncio.run(approve_revision())
    assert revised.data["approved"] is True
    history = revision_services.policy_service.repository.list_scope_history("agent", "billing-agent")
    assert {row.id: row.approved for row in history} == {policy_id: True, revision_id: True}
    assert next(row for row in history if row.id == policy_id).archived_at is not None
    assert [row.id for row in revision_services.policy_service.approved_rules("billing-agent")] == [revision_id]


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
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_ID", "Rule")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "private-lake-token")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "private-slack-token")
    database = lisa_test_database()
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    context = WorkspaceContext.from_environment()
    monkeypatch.setenv("LISA_WORKSPACE_ID", "22222222-2222-4222-8222-222222222222")
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_ID")
    services = build_services(context)
    seed_policy_document(services.policy_service, "## Rule\n\nConfirmed rule", "")
    assert context.workspace_id() == "11111111-1111-4111-8111-111111111111"
    assert "## P1.1 | Rule\napproved: no\n\nConfirmed rule" in read_policy(services)
    assert "private-lake-token" not in repr(context)
    assert "private-slack-token" not in repr(context)


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
