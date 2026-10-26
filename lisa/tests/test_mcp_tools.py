import asyncio
import json
import re
import signal
from dataclasses import replace
from pathlib import Path

import pytest
from fastmcp import Client
from policy_cases import read_policy

from benchmark.policy_seed import seed_policy_document
from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.controllers.mcp import (
    _agent_activity_minimal_projection,
    _agent_activity_thread_projection,
)
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


async def resolve_and_load_activity_memory(client, *, request_key: str, title: str) -> str:
    resolved = await client.call_tool(
        "resolve_chat_activity",
        {
            "disposition": "create",
            "request_key": request_key,
            "title": title,
            "summary": f"Handle the task: {title}.",
        },
    )
    activity_id = resolved.data["activity_id"]
    memory = await client.call_tool("load_activity_memory", {"activity_id": activity_id})
    assert memory.data["activity_id"] == activity_id
    return activity_id


async def reload_current_activity_memory(client, services) -> str:
    activity_id = services.activity_store.active_chat_activity_for_turn(
        services.context.chat_turn_id
    )
    assert activity_id is not None
    memory = await client.call_tool("load_activity_memory", {"activity_id": activity_id})
    assert memory.data["activity_id"] == activity_id
    return activity_id


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
            assert {
                "list_policies", "read_policy", "write_policy", "approve_policy",
                "list_policy_suggestions", "read_policy_suggestion",
            } <= loaded
            assert "get_session" not in loaded
            return services.conversation_repository.conversation_directory(
                services.context.workspace_id(), conversation_id
            )

    directory = asyncio.run(workflow())
    assert services.conversation_repository.context_repository(directory).loaded_namespaces() == {
        "policies"
    }


def test_thelake_namespace_and_tool_are_optional(lisa_test_context):
    import asyncio
    from dataclasses import replace

    services = build_services(lisa_test_context())

    async def unavailable():
        async with Client(create_server(services)) as client:
            found = await client.call_tool("discover_tool_namespaces", {"query": "TheLake sample"})
            assert all(item["name"] != "thelake" for item in found.data)
            await client.call_tool("load_tool_namespace", {"namespace": "thelake"}, raise_on_error=False)
            return {tool.name for tool in await client.list_tools()}

    assert asyncio.run(unavailable()) == {"discover_tool_namespaces", "load_tool_namespace"}

    class Sampler:
        def sample_recent_sessions(self, **kwargs):
            return {"queued_count": kwargs["max_sessions"]}

    services.context = replace(services.context, turn_source="scheduler")
    services.thelake_sampler = Sampler()

    async def available():
        async with Client(create_server(services)) as client:
            found = await client.call_tool("discover_tool_namespaces", {"query": "TheLake sample"})
            assert any(item["name"] == "thelake" for item in found.data)
            await client.call_tool("load_tool_namespace", {"namespace": "thelake"})
            tools = {tool.name for tool in await client.list_tools()}
            assert "sample_recent_sessions" in tools
            result = await client.call_tool("sample_recent_sessions", {"max_sessions": 3})
            assert result.data == {"queued_count": 3}

    asyncio.run(available())


def test_activity_tool_schemas_explain_turn_resolution_workflow(lisa_test_context):
    services = build_services(lisa_test_context())

    async def descriptions():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            return {tool.name: tool.description for tool in await client.list_tools()}

    tools = asyncio.run(descriptions())

    assert "search by the original task wording" in tools["find_activity"].casefold()
    assert "neutral titles such as task 1" in tools["find_activity"].casefold()
    assert "before task work" in tools["resolve_chat_activity"].casefold()
    assert "task-scoped summary" in tools["resolve_chat_activity"].casefold()
    assert "immediately call load_activity_memory" in tools["resolve_chat_activity"].casefold()
    assert "after resolving this turn" in tools["load_activity_memory"].casefold()
    assert "untrusted historical data" in tools["load_activity_memory"].casefold()


def test_load_activity_memory_requires_resolution_and_limits_reads_to_direct_parent(
    lisa_test_context,
):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    context = replace(context, chat_turn_id="activity-memory-current-turn")
    services = build_services(context)
    store = services.activity_store
    parent = store.resolve_chat_activity(
        turn_id="activity-memory-parent-turn",
        request_key="parent",
        disposition="create",
        source="web",
        title="Pine 842 payment retry",
        summary="Investigate the retry key.",
    )
    store.record_chat_turn_trajectory(
        parent.id,
        [{"id": "parent-message", "role": "user", "text": "The original key is pine-842."}],
        [],
        summary="The original key is pine-842.",
        status="succeeded",
    )
    sibling = store.resolve_chat_activity(
        turn_id="activity-memory-sibling-turn",
        request_key="sibling",
        disposition="create",
        source="web",
        title="Pine 843 payment retry",
        summary="Separate retry investigation.",
    )

    async def workflow():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            child = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "pine-842-follow-up",
                    "title": "Pine 842 retry follow-up",
                    "summary": "Check the archived retry audit.",
                    "parent_activity_id": parent.id,
                },
            )
            child_id = child.data["activity_id"]
            own_memory = await client.call_tool(
                "load_activity_memory", {"activity_id": child_id}
            )
            parent_memory = await client.call_tool(
                "load_activity_memory", {"activity_id": parent.id}
            )
            unrelated = await client.call_tool(
                "load_activity_memory",
                {"activity_id": sibling.id},
                raise_on_error=False,
            )
            return child.data, own_memory.data, parent_memory.data, unrelated

    child, own_memory, parent_memory, unrelated = asyncio.run(workflow())
    assert child["parent_activity_id"] == parent.id
    assert own_memory["activity_id"] == child["activity_id"]
    assert own_memory["status"] == "pending"
    assert parent_memory["activity_id"] == parent.id
    assert any("pine-842" in (event["summary"] or "") for event in parent_memory["events"])
    assert all("chat_turn_id" not in event for event in parent_memory["events"])
    assert unrelated.is_error
    assert "direct archived parent" in str(unrelated.content)


def test_chat_activity_writes_require_loaded_memory_for_the_resolved_activity(
    lisa_test_context,
):
    context = replace(
        lisa_test_context(
            turn_source="web",
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="activity-memory-write-guard-turn",
    )
    services = build_services(context)
    unrelated = services.activity_store.create_activity(
        title="Unrelated task", source="web", kind="chat_request"
    )

    async def workflow():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            resolved = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "memory-guard",
                    "title": "Selected task",
                    "summary": "Record the selected task.",
                },
            )
            activity_id = resolved.data["activity_id"]
            premature = await client.call_tool(
                "set_activity_progress",
                {
                    "activity_id": activity_id,
                    "status": "waiting",
                    "summary": "Must wait for memory loading.",
                },
                raise_on_error=False,
            )
            loaded = await client.call_tool(
                "load_activity_memory", {"activity_id": activity_id}
            )
            updated = await client.call_tool(
                "set_activity_progress",
                {
                    "activity_id": activity_id,
                    "status": "waiting",
                    "summary": "Memory was loaded first.",
                },
            )
            wrong_target = await client.call_tool(
                "set_activity_progress",
                {
                    "activity_id": unrelated.id,
                    "status": "waiting",
                    "summary": "Must not write to another activity.",
                },
                raise_on_error=False,
            )
            return activity_id, premature, loaded, updated, wrong_target

    activity_id, premature, loaded, updated, wrong_target = asyncio.run(workflow())
    assert premature.is_error
    assert "load_activity_memory" in str(premature.content)
    assert loaded.data["activity_id"] == activity_id
    assert not updated.is_error
    assert wrong_target.is_error
    assert "resolved activity" in str(wrong_target.content)


def test_chat_task_tools_require_loaded_memory_and_resolution_cannot_skip_it(
    lisa_test_context,
):
    context = replace(
        lisa_test_context(
            turn_source="web",
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="activity-memory-read-guard-turn",
    )
    services = build_services(context)

    async def workflow():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            # Lookup is needed to choose an activity and remains available before
            # resolution; task data reads must wait for the activity memory.
            lookup = await client.call_tool("find_activity", {"query": "selected"})
            first = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "selected-task",
                    "title": "Selected task",
                    "summary": "Review the selected task.",
                },
            )
            activity_id = first.data["activity_id"]
            guarded = []
            for name, arguments in (
                ("read_policy", {"scope": "workspace"}),
                (
                    "search_sessions",
                    {
                        "from": "2026-10-22T00:00:00Z",
                        "to": "2026-10-24T00:00:00Z",
                    },
                ),
                ("todo_read", {}),
                (
                    "lisa_db",
                    {
                        "query": (
                            Path(__file__).parent / "sql" / "activities"
                            / "read_activity_event_summary.sql"
                        ).read_text().strip()
                    },
                ),
                ("list_recent_activities", {}),
                ("find_activity", {"query": "selected"}),
                ("get_activity_thread", {"activity_id": activity_id}),
            ):
                guarded.append(
                    await client.call_tool(name, arguments, raise_on_error=False)
                )
            second_resolution = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "second-task",
                    "title": "Second task",
                    "summary": "Start another task.",
                },
                raise_on_error=False,
            )
            selected_activity_id = activity_id
            if not second_resolution.is_error:
                # Let this regression test finish on pre-fix code even though it
                # demonstrates that a second resolution bypassed the required read.
                selected_activity_id = second_resolution.data["activity_id"]
            loaded = await client.call_tool(
                "load_activity_memory", {"activity_id": selected_activity_id}
            )
            policy_read = await client.call_tool(
                "read_policy", {"scope": "workspace"}, raise_on_error=False
            )
            checklist_read = await client.call_tool(
                "todo_read", {}, raise_on_error=False
            )
            return (
                lookup,
                activity_id,
                guarded,
                second_resolution,
                loaded,
                policy_read,
                checklist_read,
            )

    (
        lookup,
        activity_id,
        guarded,
        second_resolution,
        loaded,
        policy_read,
        checklist_read,
    ) = asyncio.run(workflow())
    assert not lookup.is_error
    assert all(result.is_error for result in guarded)
    assert all("load_activity_memory" in str(result.content) for result in guarded)
    assert second_resolution.is_error
    assert "load_activity_memory" in str(second_resolution.content)
    assert loaded.data["activity_id"] == activity_id
    assert not policy_read.is_error
    assert not checklist_read.is_error


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
            memory = await client.call_tool(
                "load_activity_memory", {"activity_id": activity_id}
            )
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
            second_memory = await client.call_tool(
                "load_activity_memory", {"activity_id": second_activity_id}
            )
            recent = await client.call_tool("list_recent_activities", {"limit": 10})
            assert any(item["activity_key"] == activity_id for item in recent.data)
            updated = await client.call_tool(
                "update_activity",
                {
                    "activity_id": second_activity_id,
                    "event_type": "progress",
                    "event_title": "Policy reviewed",
                },
            )
            services.activity_store.update_activity_progress(
                second_activity_id,
                status="waiting",
                summary="Policy P1.2 is awaiting owner approval.",
                next_action="Approve policy: P1.2",
            )
            completed = await client.call_tool(
                "complete_activity",
                {
                    "activity_id": second_activity_id,
                    "status": "success",
                    "summary": "No issue found.",
                },
            )
            current = services.activity_store.get_activity_thread(second_activity_id)
            assert current["activity"]["status"] == "blocked"
            assert current["activity"]["next_action"] == "Approve policy: P1.2"
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
                chat_turn_id="test-turn-1",
            )
            assert services.activity_store.complete_chat_activity(
                activity_id,
                "Policy workflow completed.",
            )
            assert services.activity_store.complete_chat_activity(
                second_activity_id,
                "Policy workflow completed.",
            )
            thread = await client.call_tool("get_activity_thread", {"activity_id": activity_id})
            return activity_id, second_activity_id, memory, second_memory, created, updated, completed, thread

    (
        activity_id,
        second_activity_id,
        memory,
        second_memory,
        created,
        updated,
        completed,
        thread,
    ) = asyncio.run(workflow())
    assert memory.data["activity_id"] == activity_id
    assert second_memory.data["activity_id"] == second_activity_id
    assert created.data == {
        "disposition": "create",
        "activity_id": activity_id,
        "created": True,
        "request_key": "review",
    }
    assert not updated.is_error
    assert completed.data == {
        "activity_id": second_activity_id,
        "status": "deferred",
        "requested_status": "success",
        "completed": False,
        "summary": "The chat runtime will finalize this activity after Lisa's reply.",
    }
    assert thread.data["thread"]["status"] == "success"
    assert thread.data["thread"]["finish_message"] == "Policy workflow completed."
    # Shared transcript text is hidden; an explicitly targeted tool event remains.
    assert not any(
        action.get("reference_type") == "message"
        and action.get("reference_id") == "thread-user-message"
        for action in thread.data["actions"]
    )
    assert any(
        action.get("reference_type") == "runtime_event"
        and action.get("reference_id") == "thread-runtime-event"
        for action in thread.data["actions"]
    )


def test_mcp_completion_rejects_activity_when_chat_resolver_was_omitted(lisa_test_context):
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
                raise_on_error=False,
            )
        return result

    result = asyncio.run(complete_without_resolving())
    assert result.is_error
    assert "Resolve the chat activity" in str(result.content)
    assert services.activity_store.chat_activities_for_turn(context.chat_turn_id) == []


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
        lambda agent, session, *, activity_id=None, defer_activity_finalization=False: (
            calls.append((agent, session, activity_id, defer_activity_finalization))
            or {
                "activity_id": activity_id,
            }
        ),
    )

    async def evaluate():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            await client.call_tool(
                "load_activity_memory", {"activity_id": resolved.id}
            )
            return await client.call_tool(
                "evaluate_session",
                {"target_agent_name": "billing-agent", "session_id": "session-123"},
            )

    result = asyncio.run(evaluate())

    assert calls == [("billing-agent", "session-123", resolved.id, True)]
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
                {
                    "disposition": "continue",
                    "request_key": "private",
                    "summary": "Continue the private request.",
                    "activity_id": activity.id,
                },
                raise_on_error=False,
            )

    result = asyncio.run(resolve())
    assert result.is_error
    assert "authorized Lisa owner identity" in str(result.content)


def test_activity_reads_reject_non_owner(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="outsider",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Private conversation", source="web", kind="chat_request"
    )
    services.activity_store.record_activity_event(
        activity.id,
        "private_note",
        title="Private note",
        summary="CONFIDENTIAL-ACTIVITY-FACT",
    )

    async def read_activity_data():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            return [
                await client.call_tool(name, arguments, raise_on_error=False)
                for name, arguments in (
                    ("list_recent_activities", {"limit": 10}),
                    ("find_activity", {"query": "Private conversation", "open_only": False}),
                    ("get_activity_thread", {"activity_id": activity.id}),
                    (
                        "lisa_db",
                        {
                            "query": (
                                Path(__file__).parent / "sql" / "activities"
                                / "read_activity_event_summary.sql"
                            ).read_text().strip()
                        },
                    ),
                    ("load_activity_memory", {"activity_id": activity.id}),
                )
            ]

    results = asyncio.run(read_activity_data())
    assert all(result.is_error for result in results)
    assert all("authorized Lisa owner identity" in str(result.content) for result in results)


def test_agent_activity_thread_projection_is_bounded_without_truncating_saved_timeline(
    lisa_test_context,
):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Long activity", source="web", kind="chat_request"
    )
    messages = [
        {
            "id": f"long-activity-message-{index}",
            "role": "assistant",
            "text": f"Evidence {index}: " + ("x" * 2500),
            "sequence": index,
        }
        for index in range(30)
    ]
    services.activity_store.record_conversation_trajectory(activity.id, messages, [])
    full_thread = services.activity_store.get_activity_thread(activity.id)

    async def read_agent_thread():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            return await client.call_tool(
                "get_activity_thread", {"activity_id": activity.id}
            )

    result = asyncio.run(read_agent_thread())

    assert not result.is_error
    assert len(json.dumps(full_thread, default=str)) > 50_000
    assert len(full_thread["actions"]) == 31
    assert result.data["actions_truncated"] is True
    assert result.data["omitted_action_count"] == len(full_thread["actions"]) - 12
    assert len(result.data["actions"]) == 12
    assert [action["seq"] for action in result.data["actions"]] == [
        action["seq"] for action in full_thread["actions"][-12:]
    ]
    assert result.data["truncated_action_summary_count"] == 12
    assert all(len(action["summary"]) <= 1200 for action in result.data["actions"])
    assert len(json.dumps(result.data, default=str).encode("utf-8")) <= 32 * 1024
    assert len(services.activity_store.get_activity_thread(activity.id)["actions"]) == 31


def test_agent_activity_thread_projection_bounds_metadata_sections_and_references(
    lisa_test_context, monkeypatch
):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Large metadata activity", source="web", kind="chat_request"
    )
    large_value = "x" * 8000
    full_thread = {
        "activity": {
            "id": activity.id,
            "activity_key": activity.id,
            "kind": "chat_request",
            "status": "pending",
            "title": "Large metadata activity",
            "summary": "Keep useful task identity.",
            "source": "web",
            "details_json": {"private_payload": large_value},
        },
        "thread": {
            "activity_thread_id": "thread-1",
            "name": "Large metadata activity",
            "status": "pending",
            "icon": "activity",
            "details_json": {"private_payload": large_value},
        },
        "actions": [
            {
                "seq": index,
                "event_type": "progress",
                "title": f"Action {index}",
                "summary": large_value,
                "reference_id": large_value,
                "unbounded_extension": {"payload": large_value},
            }
            for index in range(30)
        ],
        "sections": [
            {
                "section_key": f"section-{index}",
                "title": large_value,
                "unbounded_extension": large_value,
            }
            for index in range(40)
        ],
        "references": [
            {
                "reference_type": "event",
                "reference_id": large_value,
                "unbounded_extension": large_value,
            }
            for _ in range(80)
        ],
    }
    monkeypatch.setattr(
        services.activity_store, "get_activity_thread", lambda _activity_id: full_thread
    )

    async def read_agent_thread():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            return await client.call_tool(
                "get_activity_thread", {"activity_id": activity.id}
            )

    result = asyncio.run(read_agent_thread())

    assert not result.is_error
    assert len(json.dumps(full_thread)) > 50_000
    assert len(json.dumps(result.data, default=str).encode("utf-8")) <= 32 * 1024
    assert result.data["activity"]["id"] == activity.id
    assert "details_json" not in result.data["activity"]
    assert "details_json" not in result.data["thread"]
    assert len(result.data["actions"]) <= 12
    assert len(result.data["sections"]) <= 12
    assert len(result.data["references"]) <= 24
    assert result.data["metadata_truncated"] is True
    assert result.data["sections_truncated"] is True
    assert result.data["references_truncated"] is True
    assert result.data["omitted_activity_field_count"] > 0
    assert result.data["omitted_thread_field_count"] > 0
    assert result.data["omitted_action_field_count"] > 0
    assert result.data["omitted_section_field_count"] > 0
    assert result.data["omitted_reference_field_count"] > 0
    assert result.data["omitted_top_level_field_count"] == 0


def test_agent_activity_thread_projection_bounds_large_unicode_metadata():
    large_text = "🧠" * 10_000
    activity = {
        key: large_text
        for key in (
            "id", "activity_key", "kind", "parent_id", "status", "title",
            "summary", "source", "created_at", "started_at", "updated_at",
            "finished_at", "next_action", "next_action_at", "origin_conversation_id",
            "origin_slack_channel_id", "origin_slack_thread_ts", "activity_type",
            "subtitle", "status_title", "task_label", "agent_id",
        )
    }
    activity["is_goal"] = True
    thread = {
        "activity": activity,
        "thread": {
            "activity_thread_id": large_text,
            "name": large_text,
            "status": large_text,
            "icon": large_text,
            "status_title": large_text,
            "finish_message": large_text,
        },
        "actions": [],
        "sections": [],
        "references": [],
    }

    def alarm_handler(_signum, _frame):
        raise TimeoutError("projection did not terminate")

    previous_handler = signal.signal(signal.SIGALRM, alarm_handler)
    signal.setitimer(signal.ITIMER_REAL, 2)
    try:
        try:
            projection = _agent_activity_thread_projection(thread)
        except TimeoutError:
            pytest.fail("projection did not terminate for large Unicode metadata")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)

    assert projection is not None
    assert len(json.dumps(projection, ensure_ascii=False, separators=(",", ":")).encode()) <= 32 * 1024
    assert projection["payload_truncated"] is True
    assert projection["metadata_truncated"] is True
    assert projection["truncated_activity_field_count"] > 0
    assert projection["truncated_thread_field_count"] > 0


def test_agent_activity_thread_minimal_projection_counts_dropped_identity_fields():
    projection = _agent_activity_minimal_projection(
        activity_source={
            "id": "activity-1",
            "kind": "chat_request",
            "status": "pending",
            "title": "A saved task",
            "summary": "Dropped after repeated size reductions.",
            "source": "web",
            "next_action": "Continue later",
        },
        thread_source={
            "activity_thread_id": "thread-1",
            "name": "A saved task",
            "status": "pending",
            "icon": "task",
            "finish_message": "Dropped after repeated size reductions.",
        },
        actions=[{"seq": 1}],
        sections=[{"section_key": "section-1"}],
        references=[{"reference_type": "event", "reference_id": "event-1"}],
        activity_details_omitted=False,
        thread_details_omitted=False,
        omitted_top_level_field_count=0,
    )

    assert len(json.dumps(projection, ensure_ascii=False, separators=(",", ":")).encode()) <= 32 * 1024
    assert projection["omitted_activity_field_count"] >= 3
    assert projection["omitted_thread_field_count"] >= 2
    assert projection["omitted_action_count"] == 1
    assert projection["omitted_section_count"] == 1
    assert projection["omitted_reference_count"] == 1


def test_agent_activity_thread_projection_reports_summary_truncated_by_budget():
    large_text = "🧠" * 400
    projection = _agent_activity_thread_projection(
        {
            "activity": {"id": "activity-1"},
            "thread": {"activity_thread_id": "thread-1"},
            "actions": [
                {
                    "seq": index,
                    "event_type": large_text,
                    "title": large_text,
                    "summary": "s" * 600,
                    "actor": large_text,
                    "reference_type": large_text,
                    "reference_id": large_text,
                    "status": large_text,
                    "created_at": large_text,
                }
                for index in range(12)
            ],
            "sections": [],
            "references": [],
        }
    )

    assert projection is not None
    assert len(projection["actions"]) == 1
    assert projection["actions"][0]["summary"] != "s" * 600
    assert projection["truncated_action_summary_count"] == len(projection["actions"])
    assert len(json.dumps(projection, ensure_ascii=False, separators=(",", ":")).encode()) <= 32 * 1024


def test_activity_writes_reject_non_owner_chat_turn(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="outsider",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Private conversation activity", source="web", kind="chat_request"
    )

    async def attempt_writes():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            return [
                await client.call_tool(name, arguments, raise_on_error=False)
                for name, arguments in (
                    (
                        "update_activity",
                        {"activity_id": activity.id, "title": "Unauthorized rewrite"},
                    ),
                    (
                        "set_activity_progress",
                        {
                            "activity_id": activity.id,
                            "status": "waiting",
                            "summary": "Unauthorized progress update.",
                        },
                    ),
                    (
                        "set_activity_section",
                        {"activity_id": activity.id, "section_key": "unauthorized"},
                    ),
                    (
                        "link_activity_reference",
                        {
                            "activity_id": activity.id,
                            "reference_type": "message",
                            "external_id": "unauthorized-message",
                        },
                    ),
                    (
                        "complete_activity",
                        {
                            "activity_id": activity.id,
                            "status": "success",
                            "summary": "Unauthorized completion.",
                        },
                    ),
                )
            ]

    results = asyncio.run(attempt_writes())
    current = services.activity_store.get_activity_thread(activity.id)

    assert all(result.is_error for result in results)
    assert all("authorized Lisa owner identity" in str(result.content) for result in results)
    assert current["activity"]["status"] == "pending"
    assert current["activity"]["title"] == "Private conversation activity"
    assert [action["event_type"] for action in current["actions"]] == ["accepted"]
    assert current["references"] == []


def test_policy_lifecycle_uses_only_human_readable_policy_id(lisa_test_context):
    context = lisa_test_context(
        turn_source="web",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    context = replace(context, chat_turn_id="policy-lifecycle-test-turn")
    services = build_services(context)

    async def create_and_find():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            await resolve_and_load_activity_memory(
                client, request_key="policy-lifecycle", title="Policy lifecycle"
            )
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
            await reload_current_activity_memory(client, services)
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
            await reload_current_activity_memory(client, services)
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
            await reload_current_activity_memory(client, services)
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
    context = replace(context, chat_turn_id="delete-policy-test-turn")
    services = build_services(context)
    record = services.policy_service.write_policy(
        scope="workspace", agent_name="", name="Retired rule", criteria="Do not use it."
    )

    async def delete():
        async with Client(create_server(services)) as client:
            await load_all_tool_namespaces(client)
            await resolve_and_load_activity_memory(
                client, request_key="delete-policy", title="Delete policy"
            )
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
    context = replace(
        lisa_test_context(
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="scoped-session-search-turn",
    )
    services = build_services(context)
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
            await resolve_and_load_activity_memory(
                client, request_key="search-sessions", title="Search sessions"
            )
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
    context = replace(
        lisa_test_context(
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="get-session-test-turn",
    )
    services = build_services(context)
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
            await resolve_and_load_activity_memory(
                client, request_key="get-session", title="Get session"
            )
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
    resolved = services.activity_store.resolve_chat_activity(
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
        lambda activity, outbox, *, defer_activity_finish=False: {
            "status": "pass",
            "stored": True,
            "activity_id": activity,
        },
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
            await client.call_tool(
                "load_activity_memory", {"activity_id": resolved.id}
            )
            return await client.call_tool(
                "evaluate_session",
                {"target_agent_name": "agent", "session_id": "session"},
            )

    result = asyncio.run(call())
    assert not result.is_error
    assert result.data["status"] == "recovered_pending_evaluations"
    assert result.data["evaluations"] == [
        {"status": "pass", "stored": True, "activity_id": "activity"}
    ]
