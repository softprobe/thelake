import asyncio
import json
import re
from dataclasses import replace
from pathlib import Path

import pytest
from fastmcp import Client
from policy_cases import read_policy

from benchmark.policy_seed import seed_policy_document
from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.mcp_app import create_server
from lisa.views.database import LISA_DATABASE_SCHEMA


async def load_all_tool_namespaces(client):
    for namespace in (
        "activities",
        "policies",
        "sessions",
        "evaluation",
        "database",
        "conversation",
    ):
        await client.call_tool("load_tool_namespace", {"namespace": namespace})


def test_mcp_public_policy_contract(lisa_test_context):
    server = create_server(build_services(lisa_test_context()))

    async def contract():
        async with Client(server) as client:
            for namespace in (
                "activities",
                "policies",
                "sessions",
                "evaluation",
                "database",
                "conversation",
            ):
                await client.call_tool("load_tool_namespace", {"namespace": namespace})
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


def test_mcp_tool_namespaces_load_on_demand_and_persist(lisa_test_context):
    import asyncio
    from uuid import uuid4

    conversation_id = str(uuid4())
    services = build_services(lisa_test_context(conversation_id=conversation_id))
    server = create_server(services)

    async def workflow():
        async with Client(server) as client:
            initial = {tool.name for tool in await client.list_tools()}
            assert initial == {"discover_tool_namespaces", "load_tool_namespace"}
            found = await client.call_tool("discover_tool_namespaces", {"query": "policy approval"})
            assert any(item["name"] == "policies" for item in found.data)
            await client.call_tool("load_tool_namespace", {"namespace": "policies"})
            loaded = {tool.name for tool in await client.list_tools()}
            assert {"list_policies", "read_policy", "write_policy", "approve_policy"} <= loaded
            assert "get_session" not in loaded
            return services.conversation_repository.conversation_directory(
                services.context.workspace_id(), conversation_id
            )

    directory = asyncio.run(workflow())
    assert services.conversation_repository.context_repository(directory).loaded_namespaces() == {
        "policies"
    }


def test_activity_tool_schemas_explain_turn_resolution_workflow(lisa_test_context):
    services = build_services(lisa_test_context())

    async def descriptions():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            return {tool.name: tool.description for tool in await client.list_tools()}

    tools = asyncio.run(descriptions())

    assert "find_activity first" in tools["find_activity"].casefold()
    assert "before any task-specific tool work" in tools["resolve_chat_activity"].casefold()
    assert "scope its later" in tools["resolve_chat_activity"].casefold()


def test_mcp_activity_tools_defer_active_chat_completion(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    context = replace(context, chat_turn_id="test-turn-1")
    services = build_services(context)
    assert services.activity_store is not None

    async def workflow():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            created = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "review",
                    "title": "Review session",
                    "summary": "Review the selected session.",
                },
            )
            activity_id = created.data["activity_id"]
            second = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "policy",
                    "title": "Review policy",
                    "summary": "Review the pending policy.",
                },
            )
            second_activity_id = second.data["activity_id"]
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
            services.activity_store.update_activity_progress(
                activity_id,
                status="waiting",
                summary="Policy P1.2 is awaiting owner approval.",
                next_action="Approve policy: P1.2",
            )
            completed = await client.call_tool(
                "complete_activity",
                {"activity_id": activity_id, "status": "success", "summary": "No issue found."},
            )
            current = services.activity_store.get_activity_thread(activity_id)
            assert current["activity"]["status"] == "blocked"
            assert current["activity"]["next_action"] == "Approve policy: P1.2"
            assert services.activity_store.complete_chat_activity(
                activity_id,
                "Policy workflow completed.",
            )
            assert services.activity_store.complete_chat_activity(
                second_activity_id,
                "Policy workflow completed.",
            )
            services.activity_store.record_conversation_trajectory(
                activity_id,
                [{"id": "thread-user-message", "role": "user", "text": "Review session."}],
                [
                    {
                        "id": "thread-runtime-event",
                        "kind": "tool",
                        "tool_name": "read_policy",
                        "sequence": 2,
                    }
                ],
            )
            thread = await client.call_tool("get_activity_thread", {"activity_id": activity_id})
            return activity_id, created, updated, completed, thread

    activity_id, created, updated, completed, thread = asyncio.run(workflow())
    assert created.data == {
        "disposition": "create",
        "activity_id": activity_id,
        "created": True,
        "request_key": "review",
    }
    assert not updated.is_error
    assert completed.data == {
        "activity_id": activity_id,
        "status": "deferred",
        "requested_status": "success",
        "completed": False,
        "summary": "The chat runtime will finalize this activity after Lisa's reply.",
    }
    assert thread.data["thread"]["status"] == "success"
    assert thread.data["thread"]["finish_message"] == "Policy workflow completed."
    assert any(
        action.get("reference_type") == "message"
        and action.get("reference_id") == "thread-user-message"
        for action in thread.data["actions"]
    )
    assert any(
        action.get("reference_type") == "runtime_event"
        and action.get("reference_id") == "thread-runtime-event"
        for action in thread.data["actions"]
    )


def test_mcp_completion_resolves_activity_when_chat_resolver_was_omitted(lisa_test_context):
    from lisa.runtime import selected_chat_activity_ids

    context = replace(
        lisa_test_context(
            turn_source="web",
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="test-turn-unresolved-completion",
    )
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Continue policy review",
        source="web",
    )

    async def complete_without_resolving():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            result = await client.call_tool(
                "complete_activity",
                {"activity_id": activity.id, "status": "success", "summary": "Done."},
            )
        return result

    result = asyncio.run(complete_without_resolving())
    assert result.data["status"] == "deferred"
    assert services.activity_store.chat_activities_for_turn(context.chat_turn_id) == [activity.id]
    assert selected_chat_activity_ids(
        [
            {
                "id": "complete-result",
                "kind": "evidence",
                "tool_name": "complete_activity",
                "detail": json.dumps(result.data),
                "sequence": 1,
            }
        ]
    ) == [activity.id]


def test_chat_evaluation_receives_the_active_resolved_activity(lisa_test_context, monkeypatch):
    context = replace(
        lisa_test_context(
            turn_source="web",
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="evaluate-session-turn",
    )
    services = build_services(context)
    resolved = services.activity_store.resolve_chat_activity(
        turn_id=context.chat_turn_id,
        request_key="review_session",
        disposition="create",
        source="web",
        title="Review billing-agent session",
        summary="Evaluate the requested session.",
    )
    calls = []
    monkeypatch.setattr(
        services.evaluations,
        "evaluate_session",
        lambda agent, session, *, activity_id=None: (
            calls.append((agent, session, activity_id))
            or {
                "activity_id": activity_id,
            }
        ),
    )

    async def evaluate():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool(
                "evaluate_session",
                {"target_agent_name": "billing-agent", "session_id": "session-123"},
            )

    result = asyncio.run(evaluate())

    assert calls == [("billing-agent", "session-123", resolved.id)]
    assert result.data["activity_id"] == resolved.id


def test_activity_chat_resolution_rejects_non_owner(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="outsider",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Private conversation",
        source="web",
    )

    async def resolve():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool(
                "resolve_chat_activity",
                {"disposition": "continue", "request_key": "private", "activity_id": activity.id},
                raise_on_error=False,
            )

    result = asyncio.run(resolve())
    assert result.is_error
    assert "authorized Lisa owner identity" in str(result.content)


def test_policy_lifecycle_uses_only_human_readable_policy_id(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)

    async def create_and_find():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            tools = {tool.name for tool in await client.list_tools()}
            assert {
                "write_policy",
                "approve_policy",
                "delete_policy",
                "list_policies",
                "read_policy",
            } <= tools
            created = await client.call_tool(
                "write_policy",
                {
                    "scope": "agent",
                    "agent_name": "billing-agent",
                    "name": "Billing changes",
                    "criteria": "Record the reason before changing billing details.",
                },
            )
            assert not created.is_error
            policy_id = created.data["policy_id"]
            assert re.fullmatch(r"P1\.[1-9][0-9]*", policy_id)
            assert created.data["approved"] is False
            found = await client.call_tool(
                "read_policy",
                {
                    "scope": "agent",
                    "agent_name": "billing-agent",
                },
            )
            assert policy_id in found.data
            assert "approved: no" in found.data
            assert "Record the reason before changing billing details." in found.data
            return policy_id

    policy_id = asyncio.run(create_and_find())

    async def approve():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool("approve_policy", {"policy_id": policy_id})

    approved = asyncio.run(approve())
    assert not approved.is_error
    assert approved.data["policy_id"] == policy_id
    assert approved.data["approved"] is True
    assert (
        services.policy_service.read_policy_records(
            "agent",
            "billing-agent",
        )[0].approved
        is True
    )

    async def update():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool(
                "write_policy",
                {
                    "scope": "agent",
                    "agent_name": "billing-agent",
                    "policy_id": policy_id,
                    "name": "Billing changes",
                    "criteria": "Record and verify the reason before changing billing details.",
                },
            )

    updated = asyncio.run(update())
    assert not updated.is_error
    updated_id = updated.data["policy_id"]
    assert updated_id == policy_id
    assert updated.data["approved"] is False
    rows = services.policy_service.read_policy_records("agent", "billing-agent")
    assert [(row.id, row.approved) for row in rows] == [(policy_id, False)]
    assert rows[0].criteria == "Record and verify the reason before changing billing details."
    assert services.policy_service.approved_rules("billing-agent") == []

    async def approve_updated_policy():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool("approve_policy", {"policy_id": policy_id})

    reapproved = asyncio.run(approve_updated_policy())
    assert reapproved.data["policy_id"] == policy_id
    assert reapproved.data["approved"] is True
    assert [row.id for row in services.policy_service.approved_rules("billing-agent")] == [
        policy_id
    ]


def test_delete_policy_executes_for_authorized_owner(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)
    record = services.policy_service.write_policy(
        scope="workspace", agent_name="", name="Retired rule", criteria="Do not use it."
    )

    async def delete():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool("delete_policy", {"policy_id": record.id})

    outsider = build_services(
        replace(
            context,
            actor_id="outsider",
        )
    )
    with pytest.raises(ValueError, match="authorized Lisa owner"):
        outsider.policy_service.delete_policy(record.id)
    assert services.policy_service.list_policy_records() == [record]

    deleted = asyncio.run(delete())
    assert not deleted.is_error
    assert deleted.data == {"policy_id": record.id, "deleted": True}
    assert services.policy_service.list_policy_records() == []


def test_mcp_delegates_to_scoped_services_and_redacts_results(monkeypatch, lisa_test_context):
    services = build_services(lisa_test_context())
    calls = []

    def search(**body):
        calls.append(body)
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
            "cursor_supported": False,
        }

    monkeypatch.setattr(services.sessions, "search", search)

    async def call():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
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
    assert calls[0] == {
        "from_time": "2026-10-11T00:00:00Z",
        "to_time": "2026-10-12T00:00:00Z",
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
        "cursor_supported": False,
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
            await load_all_tool_namespaces(client)
            return await client.call_tool("get_session", {"session_id": "session"})

    result = asyncio.run(call())
    assert not result.is_error
    assert json.loads(result.content[0].text) == trajectory


def test_context_captures_the_workspace_once(monkeypatch, lisa_test_database):
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "private-slack-token")
    database = lisa_test_database()
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    context = WorkspaceContext.from_environment()
    monkeypatch.setenv("LISA_WORKSPACE_ID", "22222222-2222-4222-8222-222222222222")
    services = build_services(context)
    seed_policy_document(services.policy_service, "## Rule\n\nConfirmed rule", "")
    assert context.workspace_id() == "11111111-1111-4111-8111-111111111111"
    assert "## P1.1 | Rule\napproved: no\n\nConfirmed rule" in read_policy(services)
    assert "private-slack-token" not in repr(context)


def test_lisa_database_tool_and_schema_resource_are_exposed(lisa_test_context):
    services = build_services(lisa_test_context())

    async def inspect_server():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            tools = await client.list_tools()
            assert any(tool.name == "lisa_db" for tool in tools)
            resource = await client.read_resource("lisa://database/schema")
            assert resource[0].text == LISA_DATABASE_SCHEMA

    asyncio.run(inspect_server())


def test_evaluate_session_recovers_pending_scores_before_policy_or_telemetry_lookup(
    monkeypatch, lisa_test_context
):
    context = replace(
        lisa_test_context(
            turn_source="web",
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="recover-pending-turn",
    )
    services = build_services(context)
    services.activity_store.resolve_chat_activity(
        turn_id=context.chat_turn_id,
        request_key="recover_session",
        disposition="create",
        source="web",
        title="Recover pending evaluation",
        summary="Reuse the stored evaluation result.",
    )
    monkeypatch.setattr(
        services.evaluations.activities,
        "find_pending_evaluations_for_session",
        lambda agent, session: (
            [("activity", "outbox")] if (agent, session) == ("agent", "session") else []
        ),
    )
    monkeypatch.setattr(
        services.evaluations,
        "_recover_pending_score",
        lambda activity, outbox: {"status": "pass", "activity_id": activity},
    )
    monkeypatch.setattr(
        services.sessions.traces,
        "load",
        lambda _path: pytest.fail("Recovery must not fetch live telemetry"),
    )
    monkeypatch.setattr(
        services.evaluations.policy_triage,
        "triage",
        lambda *_args: pytest.fail("Recovery must not run policy selection"),
    )

    async def call():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return await client.call_tool(
                "evaluate_session",
                {"target_agent_name": "agent", "session_id": "session"},
            )

    result = asyncio.run(call())
    assert not result.is_error
    assert result.data["status"] == "recovered_pending_evaluations"
    assert result.data["evaluations"] == [{"status": "pass", "activity_id": "activity"}]
