from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, BrokenBarrierError
from types import SimpleNamespace

import psycopg
import pytest

from lisa.activity_memory import activity_memory_configuration_fingerprint
from lisa.bootstrap import build_services
from lisa.services import conversation_checkpoints
from lisa.services.conversation_checkpoints import ConversationCheckpointWorker


def _chat_activity(store, turn_id: str, key: str, title: str, *, parent_id=None):
    return store.resolve_chat_activity(
        turn_id=turn_id,
        request_key=key,
        disposition="create",
        source="web",
        title=title,
        summary=f"Track {title}.",
        parent_activity_id=parent_id,
    )


def _record(store, activity_id: str, text: str, *, status="waiting"):
    store.record_chat_turn_trajectory(
        activity_id,
        [{"id": f"message-{text}", "role": "user", "text": text, "sequence": 1}],
        [],
        summary=text,
        status=status,
        next_action="Continue the investigation" if status == "waiting" else None,
    )


def _test_sql(name: str) -> str:
    return (Path(__file__).parent / "sql" / "activities" / name).read_text()


def test_completed_chat_activity_continues_as_child_without_reopening_parent(lisa_test_context):
    services = build_services(lisa_test_context())
    store = services.activity_store
    parent = _chat_activity(store, "parent-turn", "initial", "Pine 842 retry audit")
    _record(store, parent.id, "The original retry used key pine-842.", status="succeeded")
    before = store.get_activity_thread(parent.id)

    child = _chat_activity(
        store,
        "child-turn",
        "continuation",
        "Pine 842 retry audit follow-up",
        parent_id=parent.id,
    )

    after = store.get_activity_thread(parent.id)
    child_thread = store.get_activity_thread(child.id)
    assert child.id != parent.id
    assert child_thread["activity"]["parent_id"] == parent.id
    assert after["activity"]["status"] == "success"
    assert after["activity"]["summary"] == before["activity"]["summary"]
    assert [item["event_type"] for item in after["actions"]] == [
        item["event_type"] for item in before["actions"]
    ]


def test_activity_memory_publication_is_idempotent_monotonic_and_terminally_frozen(
    lisa_test_context,
):
    context = lisa_test_context()
    store = build_services(context).activity_store
    activity = _chat_activity(store, "memory-turn", "task", "Orchid 731 invoice review")
    _record(store, activity.id, "The corrected total is 127 dollars.")
    config = activity_memory_configuration_fingerprint("test-model")
    first = store.capture_activity_memory(activity.id, 2, config)

    published = store.publish_activity_memory(first, "The corrected total is $127.")
    duplicate = store.publish_activity_memory(first, "A different retry must not replace it.")
    stale = store.publish_activity_memory(
        {**first, "event_watermark": 1, "source_event_refs": []},
        "An older snapshot must not become current.",
    )

    assert published["published"] is True
    assert duplicate["id"] == published["id"]
    assert duplicate["summary"] == "The corrected total is $127."
    assert stale["published"] is False
    assert stale["stale"] is True

    _record(store, activity.id, "The reviewer accepted the corrected amount.", status="succeeded")
    detail = store.get_activity_thread(activity.id)
    final_watermark = max(action["seq"] for action in detail["actions"])
    final_payload = store.capture_activity_memory(activity.id, final_watermark, config)
    final = store.publish_activity_memory(final_payload, "Final review accepted.", terminal=True)
    assert final["published"] is True
    assert final["is_terminal"] is True
    assert final["source_event_seq"] == final_watermark
    with pytest.raises(ValueError, match="terminal watermark"):
        store.publish_activity_memory(
            {**final_payload, "event_watermark": final_watermark - 1},
            "A terminal snapshot at the wrong boundary.",
            terminal=True,
        )


def test_concurrent_activity_memory_writers_publish_one_immutable_artifact(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    activity = _chat_activity(store, "concurrent-memory-turn", "task", "Cache 166 invalidation")
    _record(store, activity.id, "Region west missed invalidation event 166.")
    config = activity_memory_configuration_fingerprint("test-model")
    payload = store.capture_activity_memory(activity.id, 2, config)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda summary: store.publish_activity_memory(payload, summary),
                ("First complete snapshot.", "Concurrent alternate snapshot."),
            )
        )

    assert results[0]["id"] == results[1]["id"]
    assert results[0]["summary"] == results[1]["summary"]
    assert store.get_activity_memory_artifact(activity.id, 2, config)["id"] == results[0]["id"]


def test_multiactivity_turn_memory_excludes_the_shared_cross_task_transcript(
    lisa_test_context,
):
    store = build_services(lisa_test_context()).activity_store
    first = store.resolve_chat_activity(
        turn_id="interleaved-turn",
        request_key="first-task",
        disposition="create",
        source="web",
        title="Invoice 593 tax mismatch",
        summary="Invoice 593: use the corrected tax total of 19.35.",
    )
    second = store.resolve_chat_activity(
        turn_id="interleaved-turn",
        request_key="second-task",
        disposition="create",
        source="web",
        title="Invoice 594 duplicate charge",
        summary="Invoice 594: provider reference is charge-594-R.",
    )
    store.record_activity_event(
        first.id,
        "note",
        title="Corrected tax",
        summary="Invoice 593 corrected tax is 19.35.",
        actor="user",
    )
    store.record_activity_event(
        second.id,
        "note",
        title="Provider reference",
        summary="Invoice 594 provider reference is charge-594-R.",
        actor="user",
    )
    shared_messages = [
        {
            "id": "interleaved-user-message",
            "role": "user",
            "text": "Invoice 593 corrected tax is 19.35. Invoice 594 provider reference is charge-594-R.",
            "sequence": 1,
        },
        {
            "id": "interleaved-assistant-message",
            "role": "assistant",
            "text": "I will keep both invoice tasks separate.",
            "sequence": 2,
        },
    ]
    for activity in (first, second):
        task_label = "ORCHID" if activity.id == first.id else "MAPLE"
        store.record_chat_turn_trajectory(
            activity.id,
            shared_messages,
            [],
            summary="Both invoice requests were captured.",
            status="waiting",
            next_action="Continue this invoice task.",
            chat_turn_id="interleaved-turn",
        )

    config = activity_memory_configuration_fingerprint("test-model")
    first_watermark = max(event.sequence for event in store.list_events(first.id))
    second_watermark = max(event.sequence for event in store.list_events(second.id))
    first_memory = store.capture_activity_memory(first.id, first_watermark, config)
    second_memory = store.capture_activity_memory(second.id, second_watermark, config)

    assert "Invoice 593" in first_memory["messages"]
    assert "19.35" in first_memory["messages"]
    assert "Invoice 594" not in first_memory["messages"]
    assert "charge-594-R" not in first_memory["messages"]
    assert "Invoice 594" in second_memory["messages"]
    assert "charge-594-R" in second_memory["messages"]
    assert "Invoice 593" not in second_memory["messages"]
    assert "19.35" not in second_memory["messages"]
    first_thread_text = str(store.get_activity_thread(first.id)["actions"])
    second_thread_text = str(store.get_activity_thread(second.id)["actions"])
    assert "Invoice 594 provider reference" not in first_thread_text
    assert "charge-594-R" not in first_thread_text
    assert "Invoice 593 corrected tax" not in second_thread_text
    assert "19.35" not in second_thread_text


def test_failed_multiactivity_turn_keeps_shared_user_text_out_of_each_thread(
    lisa_test_context,
):
    store = build_services(lisa_test_context()).activity_store
    first = _chat_activity(store, "failed-interleaved-turn", "first", "Invoice 851 tax review")
    second = _chat_activity(store, "failed-interleaved-turn", "second", "Invoice 852 charge review")

    store.fail_chat_turn(
        "failed-interleaved-turn",
        "Invoice 851 tax is 8.50; invoice 852 charge is CHARGE-852.",
        "The provider timed out.",
    )

    first_thread_text = str(store.get_activity_thread(first.id)["actions"])
    second_thread_text = str(store.get_activity_thread(second.id)["actions"])
    assert "CHARGE-852" not in first_thread_text
    assert "Invoice 851 tax is 8.50" not in second_thread_text


def test_multiactivity_turn_summaries_do_not_enter_activity_memory(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    first = store.resolve_chat_activity(
        turn_id="summary-leak-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Orchid summary isolation",
        summary="Orchid task facts; foreign Maple value MAPLE-SUMMARY-SECRET.",
    )
    second = store.resolve_chat_activity(
        turn_id="summary-leak-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple summary isolation",
        summary="Maple task facts; foreign Orchid value ORCHID-SUMMARY-SECRET.",
    )
    messages = [
        {
            "id": "summary-leak-user",
            "role": "user",
            "text": (
                "Orchid task facts. Maple task facts. "
                "MAPLE-USER-SECRET ORCHID-USER-SECRET."
            ),
            "sequence": 1,
        }
    ]
    store.record_chat_turn_trajectory(
        first.id,
        messages,
        [],
        summary="Orchid progress; foreign Maple value MAPLE-PROGRESS-SECRET.",
        status="waiting",
        next_action="Continue Orchid.",
        chat_turn_id="summary-leak-turn",
    )
    store.record_chat_turn_trajectory(
        second.id,
        messages,
        [],
        summary="Maple progress; foreign Orchid value ORCHID-PROGRESS-SECRET.",
        status="waiting",
        next_action="Continue Maple.",
        chat_turn_id="summary-leak-turn",
    )

    config = activity_memory_configuration_fingerprint("test-model")
    memories = {
        first.id: store.capture_activity_memory(
            first.id, max(event.sequence for event in store.list_events(first.id)), config
        ),
        second.id: store.capture_activity_memory(
            second.id, max(event.sequence for event in store.list_events(second.id)), config
        ),
    }
    threads = {
        first.id: str(store.get_activity_thread(first.id)),
        second.id: str(store.get_activity_thread(second.id)),
    }
    feed = str(store.list_activity_feed(query="summary isolation", open_only=True))

    for activity_id, forbidden in (
        (
            first.id,
            (
                "MAPLE-SUMMARY-SECRET",
                "MAPLE-PROGRESS-SECRET",
                "MAPLE-USER-SECRET",
                "ORCHID-USER-SECRET",
            ),
        ),
        (
            second.id,
            (
                "ORCHID-SUMMARY-SECRET",
                "ORCHID-PROGRESS-SECRET",
                "MAPLE-USER-SECRET",
                "ORCHID-USER-SECRET",
            ),
        ),
    ):
        combined = memories[activity_id]["messages"] + threads[activity_id] + feed
        for phrase in forbidden:
            assert phrase not in combined


def test_multiactivity_turn_neutralizes_progress_titles_written_before_second_resolution(
    lisa_test_context,
):
    store = build_services(lisa_test_context()).activity_store
    turn_id = "progress-title-leak-turn"
    first = store.resolve_chat_activity(
        turn_id=turn_id,
        request_key="orchid",
        disposition="create",
        source="web",
        title="Orchid work - MAPLE-PROGRESS-TITLE-SECRET",
        summary="Orchid facts; Maple-only value MAPLE-PROGRESS-TITLE-SECRET.",
    )
    store.update_activity_progress(
        first.id,
        status="waiting",
        summary="Orchid work remains waiting.",
        next_action="Wait for an Orchid follow-up.",
        chat_turn_id=turn_id,
    )
    second = store.resolve_chat_activity(
        turn_id=turn_id,
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple work - ORCHID-PROGRESS-TITLE-SECRET",
        summary="Maple facts; Orchid-only value ORCHID-PROGRESS-TITLE-SECRET.",
    )
    store.update_activity_progress(
        second.id,
        status="waiting",
        summary="Maple work remains waiting.",
        next_action="Wait for a Maple follow-up.",
        chat_turn_id=turn_id,
    )
    for activity, other_task_secret, task_key in (
        (first, "MAPLE-PROGRESS-TITLE-SECRET", "orchid"),
        (second, "ORCHID-PROGRESS-TITLE-SECRET", "maple"),
    ):
        tool_call_id = f"{task_key}-update-activity-call"
        store.record_chat_turn_trajectory(
            activity.id,
            [],
            [
                {
                    "id": f"{task_key}-update-activity-tool",
                    "kind": "tool",
                    "tool_name": "update_activity",
                    "arguments": {
                        "activity_id": activity.id,
                        "title": f"{task_key.title()} activity {other_task_secret}",
                    },
                    "tool_call_id": tool_call_id,
                    "sequence": 1,
                },
                {
                    "id": f"{task_key}-update-activity-result",
                    "kind": "evidence",
                    "tool_name": "update_activity",
                    "detail": "{\"activity_id\": \"updated\"}",
                    "tool_call_id": tool_call_id,
                    "sequence": 2,
                },
            ],
            summary="The selected work remains waiting.",
            status="waiting",
            next_action="Wait for the next follow-up.",
            chat_turn_id=turn_id,
        )

    exposed = []
    configuration = activity_memory_configuration_fingerprint("test-model")
    for activity_id in (first.id, second.id):
        thread = store.get_activity_thread(activity_id)
        watermark = max(action["seq"] for action in thread["actions"])
        memory = store.capture_activity_memory(activity_id, watermark, configuration)
        exposed.extend((thread, memory["messages"]))

    combined = str(exposed)
    assert "MAPLE-PROGRESS-TITLE-SECRET" not in combined
    assert "ORCHID-PROGRESS-TITLE-SECRET" not in combined
    assert "Orchid work remains waiting." in str(exposed[1])
    assert "Maple work remains waiting." in str(exposed[3])


def test_pending_activity_memory_uses_the_same_cross_task_projection(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    first = store.resolve_chat_activity(
        turn_id="pending-summary-leak-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Orchid pending memory",
        summary="Orchid request; foreign Maple value MAPLE-PENDING-SECRET.",
    )
    second = store.resolve_chat_activity(
        turn_id="pending-summary-leak-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple pending memory",
        summary="Maple request; foreign Orchid value ORCHID-PENDING-SECRET.",
    )
    shared_messages = [
        {
            "id": "pending-shared-user",
            "role": "user",
            "text": "MAPLE-PENDING-USER and ORCHID-PENDING-USER",
            "sequence": 1,
        },
        {
            "id": "pending-shared-assistant",
            "role": "assistant",
            "text": "MAPLE-PENDING-ASSISTANT and ORCHID-PENDING-ASSISTANT",
            "sequence": 2,
        },
    ]
    for activity in (first, second):
        task_label = "ORCHID" if activity.id == first.id else "MAPLE"
        store.record_chat_turn_trajectory(
            activity.id,
            shared_messages,
            [
                {
                    "id": f"pending-tool-{activity.id}",
                    "kind": "tool",
                    "tool_name": "lookup",
                    "arguments": {"query": f"{task_label}-PENDING-TOOL"},
                    "sequence": 3,
                },
                {
                    "id": f"pending-result-{activity.id}",
                    "kind": "evidence",
                    "tool_name": "lookup",
                    "detail": f"{task_label}-PENDING-RESULT",
                    "sequence": 4,
                },
            ],
            summary="MAPLE-PENDING-PROGRESS and ORCHID-PENDING-PROGRESS",
            status="waiting",
            next_action="Continue the selected task.",
            chat_turn_id="pending-summary-leak-turn",
        )

    memory = store.load_activity_memory_for_turn(first.id, first.id)

    assert memory["status"] == "pending"
    assert all(event["event_type"] not in {"chat.request", "chat.user", "chat.assistant"}
               for event in memory["events"])
    serialized = str(memory)
    for forbidden in (
        "MAPLE-PENDING-SECRET",
        "ORCHID-PENDING-SECRET",
        "MAPLE-PENDING-USER",
        "ORCHID-PENDING-USER",
        "MAPLE-PENDING-ASSISTANT",
        "ORCHID-PENDING-ASSISTANT",
        "MAPLE-PENDING-TOOL",
        "MAPLE-PENDING-RESULT",
        "MAPLE-PENDING-PROGRESS",
        "ORCHID-PENDING-PROGRESS",
    ):
        assert forbidden not in serialized
    assert "ORCHID-PENDING-TOOL" in serialized
    assert "ORCHID-PENDING-RESULT" in serialized


def test_resolver_payloads_are_excluded_from_saved_thread_and_memory_projections(
    lisa_test_context,
):
    store = build_services(lisa_test_context()).activity_store
    activity = _chat_activity(store, "resolver-projection-turn", "orchid", "Orchid review")
    store.record_conversation_trajectory(
        activity.id,
        [],
        [
            {
                "id": "resolver-projection-call",
                "kind": "tool",
                "tool_name": "mcp_lisa.resolve_chat_activity",
                "arguments": {"summary": "Foreign task value MAPLE-RESOLVER-SECRET"},
                "sequence": 1,
            },
            {
                "id": "resolver-projection-result",
                "kind": "evidence",
                "tool_name": "mcp_lisa.resolve_chat_activity",
                "detail": json.dumps({
                    "activity_id": activity.id,
                    "summary": "Foreign task value MAPLE-RESOLVER-SECRET",
                }),
                "sequence": 2,
            },
            {
                "id": "resolver-projection-task-result",
                "kind": "evidence",
                "tool_name": "read_task_fact",
                "detail": "Selected task value ORCHID-TASK-FACT",
                "sequence": 3,
            },
        ],
    )

    thread = store.get_activity_thread(activity.id)
    memory = store.load_activity_memory_for_turn(activity.id, activity.id)
    combined = str({"thread": thread, "memory": memory})

    assert "MAPLE-RESOLVER-SECRET" not in combined
    assert "ORCHID-TASK-FACT" in combined
    assert {
        item["reference_id"] for item in thread["references"]
    }.isdisjoint({"resolver-projection-call", "resolver-projection-result"})


def test_mcp_activity_progress_write_is_kept_in_its_task_memory(lisa_test_context):
    import asyncio
    from dataclasses import replace

    from fastmcp import Client

    from lisa.mcp_app import create_server

    context = replace(
        lisa_test_context(
            turn_source="web",
            actor_id="owner-1",
            owner_user_ids=frozenset({"owner-1"}),
        ),
        chat_turn_id="mcp-progress-isolation-turn",
    )
    services = build_services(context)

    async def write_task_progress():
        async with Client(create_server(services)) as client:
            await client.call_tool("load_tool_namespace", {"namespace": "activities"})
            first_result = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "orchid-progress",
                    "title": "Orchid progress memory",
                    "summary": "Track the Orchid launch review.",
                },
            )
            first_id = first_result.data["activity_id"]
            first_memory = await client.call_tool(
                "load_activity_memory", {"activity_id": first_id}
            )
            progress = await client.call_tool(
                "set_activity_progress",
                {
                    "activity_id": first_id,
                    "status": "waiting",
                    "summary": "ORCHID-ONLY-PROGRESS-742 is awaiting confirmation.",
                    "next_action": "Confirm ORCHID-ONLY-PROGRESS-742.",
                },
            )
            second_result = await client.call_tool(
                "resolve_chat_activity",
                {
                    "disposition": "create",
                    "request_key": "maple-progress",
                    "title": "Maple progress memory",
                    "summary": "Track the Maple vendor review.",
                },
            )
            second_id = second_result.data["activity_id"]
            second_memory = await client.call_tool(
                "load_activity_memory", {"activity_id": second_id}
            )
        return first_id, second_id, first_memory, second_memory, progress

    first_id, second_id, first_memory, second_memory, progress = asyncio.run(
        write_task_progress()
    )
    assert first_memory.data["activity_id"] == first_id
    assert second_memory.data["activity_id"] == second_id
    assert not progress.is_error

    def captured(activity_id):
        watermark = max(event.sequence for event in services.activity_store.list_events(activity_id))
        return services.activity_store.capture_activity_memory(
            activity_id, watermark, "test-configuration"
        )["messages"]

    first_memory = captured(first_id)
    second_memory = captured(second_id)
    assert "ORCHID-ONLY-PROGRESS-742" in first_memory
    assert "ORCHID-ONLY-PROGRESS-742" not in second_memory


def test_multi_activity_titles_are_private_but_remain_searchable(lisa_test_context):
    context = lisa_test_context()
    store = build_services(context).activity_store
    first = store.resolve_chat_activity(
        turn_id="title-leak-turn",
        request_key="orchid-title",
        disposition="create",
        source="web",
        title="Orchid 731 review with foreign Maple ID MAPLE-TITLE-SECRET",
        summary="Review Orchid invoice 731.",
    )
    store.record_activity_event(
        first.id, "note", title="Invoice 731", summary="Orchid invoice 731 is under review."
    )
    configuration = "title-isolation-configuration"
    before_second_resolution = store.capture_activity_memory(
        first.id,
        max(event.sequence for event in store.list_events(first.id)),
        configuration,
    )
    store.publish_activity_memory(
        before_second_resolution,
        "A stale summary repeats MAPLE-TITLE-SECRET.",
    )
    second = store.resolve_chat_activity(
        turn_id="title-leak-turn",
        request_key="maple-title",
        disposition="create",
        source="web",
        title="Maple 844 review with foreign Orchid ID ORCHID-TITLE-SECRET",
        summary="Review Maple shipment 844.",
    )

    exposed = []
    for activity in (first, second):
        thread = store.get_activity_thread(activity.id)
        watermark = max(event.sequence for event in store.list_events(activity.id))
        memory = store.capture_activity_memory(
            activity.id, watermark, configuration
        )
        exposed.extend((thread, memory["messages"]))
    pending = store.load_activity_memory_for_turn(first.id, first.id)
    store.record_activity_event(
        first.id,
        "note",
        title="Follow-up evidence",
        summary="Orchid invoice 731 is awaiting confirmation.",
    )
    rebuilt = store.capture_activity_memory(
        first.id,
        max(event.sequence for event in store.list_events(first.id)),
        configuration,
    )
    feed = [
        item
        for item in store.list_activity_feed(limit=100)
        if item["id"] in {first.id, second.id}
    ]
    exposed.append(feed)
    exposed.extend((pending, rebuilt["messages"], rebuilt["base_summary"]))
    with psycopg.connect(context.database_read_url) as connection:
        raw_event_rows = connection.execute(
            _test_sql("read_activity_event_summaries.sql"),
            ([first.id, second.id],),
        ).fetchall()
    exposed.append(raw_event_rows)

    assert "MAPLE-TITLE-SECRET" not in str(exposed)
    assert "ORCHID-TITLE-SECRET" not in str(exposed)
    assert pending["status"] == "pending"
    assert pending["snapshot"] is None
    assert rebuilt["base_summary"] is None
    assert {item["title"] for item in feed} == {"Task 1", "Task 2"}
    assert [item["id"] for item in store.list_activity_feed(query="Orchid 731 review")] == [first.id]
    assert [item["id"] for item in store.list_activity_feed(query="Maple 844 review")] == [second.id]


def test_shared_turn_neutralization_survives_later_presentation_writes(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    first = store.resolve_chat_activity(
        turn_id="later-title-write-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Orchid title write",
        summary="Orchid task summary.",
    )
    store.resolve_chat_activity(
        turn_id="later-title-write-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple title write",
        summary="Maple task summary.",
    )

    store.update_activity_presentation(
        first.id,
        title="Orchid follow-up with MAPLE-LATER-TITLE-SECRET",
        subtitle="MAPLE-LATER-SUBTITLE-SECRET",
        status_title="MAPLE-LATER-STATUS-SECRET",
        task_label="MAPLE-LATER-LABEL-SECRET",
        details={"foreign_task": "MAPLE-LATER-DETAIL-SECRET"},
    )
    feed = store.list_activity_feed(query="MAPLE-LATER-TITLE-SECRET")
    thread = store.get_activity_thread(first.id)
    exposed = str((feed, thread))

    assert len(feed) == 1
    assert feed[0]["title"] == "Task 1"
    assert "MAPLE-LATER-TITLE-SECRET" not in exposed
    assert "MAPLE-LATER-SUBTITLE-SECRET" not in exposed
    assert "MAPLE-LATER-STATUS-SECRET" not in exposed
    assert "MAPLE-LATER-LABEL-SECRET" not in exposed
    assert "MAPLE-LATER-DETAIL-SECRET" not in exposed


def test_shared_turn_completion_does_not_restore_cross_activity_summary(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    first = store.resolve_chat_activity(
        turn_id="shared-completion-summary-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Orchid completion",
        summary="Orchid request.",
    )
    store.resolve_chat_activity(
        turn_id="shared-completion-summary-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple completion",
        summary="Maple request.",
    )

    store.record_chat_turn_trajectory(
        first.id,
        [],
        [],
        summary="Orchid completed; Maple-only value MAPLE-COMPLETION-SECRET.",
        status="succeeded",
        chat_turn_id="shared-completion-summary-turn",
    )
    feed = store.list_activity_feed(query="Orchid completion")
    thread = store.get_activity_thread(first.id)

    assert len(feed) == 1
    assert feed[0]["summary"] == "Task 1"
    assert "MAPLE-COMPLETION-SECRET" not in str(thread)


def test_snapshot_captured_before_neutralization_is_rejected_even_if_title_matches(
    lisa_test_context,
):
    store = build_services(lisa_test_context()).activity_store
    first = store.resolve_chat_activity(
        turn_id="same-neutral-title-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Task 1",
        summary="Task 1 is the original display title.",
    )
    watermark = max(event.sequence for event in store.list_events(first.id))
    captured = store.capture_activity_memory(
        first.id, watermark, "same-neutral-title-configuration"
    )
    store.publish_activity_memory(captured, "MAPLE-STALE-SNAPSHOT-SECRET")

    store.resolve_chat_activity(
        turn_id="same-neutral-title-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple second task",
        summary="Maple task summary.",
    )
    memory = store.load_activity_memory_for_turn(first.id, first.id)

    assert memory["status"] == "pending"
    assert memory["snapshot"] is None
    assert "MAPLE-STALE-SNAPSHOT-SECRET" not in str(memory)


def test_neutralization_allows_safe_snapshot_at_the_same_watermark(lisa_test_context):
    context = lisa_test_context()
    store = build_services(context).activity_store
    first = store.resolve_chat_activity(
        turn_id="same-watermark-safe-snapshot-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Task 1",
        summary="The original display title is already neutral-looking.",
    )
    watermark = max(event.sequence for event in store.list_events(first.id))
    stale_payload = store.capture_activity_memory(
        first.id, watermark, "same-watermark-configuration"
    )
    stale = store.publish_activity_memory(stale_payload, "PRE-NEUTRALIZATION-SECRET")

    store.resolve_chat_activity(
        turn_id="same-watermark-safe-snapshot-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple second task",
        summary="Maple task summary.",
    )
    stale_retry = store.publish_activity_memory(
        stale_payload, "A late pre-neutralization worker result."
    )
    with psycopg.connect(context.database_url) as connection:
        queued = connection.execute(
            _test_sql("read_activity_memory_queue_messages.sql"),
            (first.id,),
        ).fetchall()
    safe_payload = store.capture_activity_memory(
        first.id, watermark, "same-watermark-configuration"
    )
    safe = store.publish_activity_memory(safe_payload, "Safe neutralized snapshot.")
    loaded = store.load_activity_memory_for_turn(first.id, first.id)

    assert safe["id"] != stale["id"]
    assert safe["published"] is True
    assert stale_retry["stale"] is True
    assert any(row[0]["title_neutralized"] is True for row in queued)
    assert loaded["status"] == "ready"
    assert loaded["snapshot"]["summary"] == "Safe neutralized snapshot."
    assert "PRE-NEUTRALIZATION-SECRET" not in str(loaded)


def test_conversation_trajectory_failure_path_sanitizes_readable_events(lisa_test_context):
    context = lisa_test_context()
    store = build_services(context).activity_store
    first = store.resolve_chat_activity(
        turn_id="inactive-failure-shared-turn",
        request_key="orchid",
        disposition="create",
        source="web",
        title="Orchid request",
        summary="Orchid request summary.",
    )
    store.resolve_chat_activity(
        turn_id="inactive-failure-shared-turn",
        request_key="maple",
        disposition="create",
        source="web",
        title="Maple request",
        summary="Maple request summary.",
    )

    store.record_conversation_trajectory(
        first.id,
        [{
            "id": "inactive-failure-user-message",
            "role": "user",
            "text": "MAPLE-INACTIVE-FAILURE-SECRET",
            "sequence": 1,
        }],
        [],
        chat_turn_id="inactive-failure-shared-turn",
    )
    with psycopg.connect(context.database_read_url) as connection:
        rows = connection.execute(
            _test_sql("read_chat_user_activity_events.sql"),
            (first.id,),
        ).fetchall()

    assert len(rows) == 1
    assert rows[0][1] == "Shared chat context withheld"
    assert rows[0][2] is None
    assert "MAPLE-INACTIVE-FAILURE-SECRET" not in str(rows)


def test_activity_resolution_serializes_distinct_requests_by_chat_turn(
    lisa_test_context, monkeypatch
):
    store = build_services(lisa_test_context()).activity_store
    both_resolvers_entered = Barrier(2)
    original_sanitize = getattr(
        store, "_sanitize_shared_chat_activity_summaries", None
    )

    def wait_for_competing_resolver(connection, turn_id):
        try:
            both_resolvers_entered.wait(timeout=0.4)
        except BrokenBarrierError:
            pass
        if original_sanitize is not None:
            return original_sanitize(connection, turn_id)

    monkeypatch.setattr(
        store,
        "_sanitize_shared_chat_activity_summaries",
        wait_for_competing_resolver,
        raising=False,
    )

    def resolve(key, title, summary):
        return store.resolve_chat_activity(
            turn_id="concurrent-summary-sanitization",
            request_key=key,
            disposition="create",
            source="web",
            title=title,
            summary=summary,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        activities = list(
            executor.map(
                lambda item: resolve(*item),
                (
                    (
                        "orchid",
                        "Orchid concurrent summary",
                        "Orchid work; foreign Maple CONCURRENT-MAPLE-SECRET.",
                    ),
                    (
                        "maple",
                        "Maple concurrent summary",
                        "Maple work; foreign Orchid CONCURRENT-ORCHID-SECRET.",
                    ),
                ),
            )
        )

    visible_feed = str(
        [
            entry
            for entry in store.list_activity_feed(query="concurrent summary", open_only=True)
            if entry["activity_key"] in {activity.id for activity in activities}
        ]
    )
    assert "CONCURRENT-MAPLE-SECRET" not in visible_feed
    assert "CONCURRENT-ORCHID-SECRET" not in visible_feed


def test_completed_activity_is_read_only_across_activity_write_paths(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    parent = _chat_activity(store, "immutable-parent-turn", "task", "Terminal 919 invoice audit")
    _record(store, parent.id, "The invoice was approved.", status="succeeded")
    before = store.get_activity_thread(parent.id)

    operations = (
        lambda: store.update_activity_presentation(parent.id, title="Changed parent"),
        lambda: store.record_activity_event(
            parent.id, "note", title="Changed parent", summary="This must be rejected."
        ),
        lambda: store.set_activity_section(parent.id, "follow-up", title="Follow-up"),
        lambda: store.link_activity_reference(parent.id, "runtime_event", "late-event"),
    )
    for operation in operations:
        with pytest.raises(ValueError, match="completed chat activities are read-only"):
            operation()

    after = store.get_activity_thread(parent.id)
    assert after["activity"]["title"] == before["activity"]["title"]
    assert after["actions"] == before["actions"]
    assert after["sections"] == before["sections"]
    assert after["references"] == before["references"]


def test_activity_memory_loader_only_reads_resolved_activity_or_direct_terminal_parent(
    lisa_test_context,
):
    context = lisa_test_context(turn_source="web", actor_id="owner-1", owner_user_ids=frozenset({"owner-1"}))
    store = build_services(context).activity_store
    parent = _chat_activity(store, "memory-parent", "parent", "Maple 244 service retry")
    _record(store, parent.id, "Service retry failed with code E244.", status="succeeded")
    child = _chat_activity(
        store, "memory-child", "child", "Maple 244 retry follow-up", parent_id=parent.id
    )
    _record(store, child.id, "The retry was blocked by the circuit breaker.")
    sibling = _chat_activity(store, "memory-sibling", "sibling", "Maple 245 gateway retry")

    current = store.load_activity_memory_for_turn(child.id, child.id)
    archived_parent = store.load_activity_memory_for_turn(parent.id, child.id)
    with pytest.raises(ValueError, match="not the resolved activity or its direct archived parent"):
        store.load_activity_memory_for_turn(sibling.id, child.id)

    assert current["activity_id"] == child.id
    assert current["status"] == "pending"
    assert any("circuit breaker" in (event["summary"] or "") for event in current["events"])
    assert all("chat_turn_id" not in event for event in current["events"])
    assert archived_parent["activity_id"] == parent.id
    assert archived_parent["status"] == "pending"
    assert all(event["activity_id"] == parent.id for event in archived_parent["events"])


def test_pending_activity_memory_fallback_is_bounded_by_text_size(lisa_test_context):
    store = build_services(lisa_test_context()).activity_store
    activity = _chat_activity(store, "large-memory-turn", "task", "Large 918 evidence file")
    store.record_conversation_trajectory(
        activity.id,
        [
            {"id": "large-message-1", "role": "user", "text": "A" * 12_000, "sequence": 1},
            {"id": "large-message-2", "role": "assistant", "text": "B" * 12_000, "sequence": 2},
        ],
        [],
        chat_turn_id="large-memory-turn",
    )

    memory = store.load_activity_memory_for_turn(activity.id, activity.id)

    assert memory["status"] == "pending"
    assert memory["fallback_truncated"] is True
    assert sum(
        len(event["title"]) + len(event["summary"] or "")
        for event in memory["events"]
    ) <= 12_000


def test_chat_event_commit_enqueues_activity_memory_at_exact_terminal_watermark(lisa_test_context):
    context = lisa_test_context()
    services = build_services(context)
    store = services.activity_store
    activity = _chat_activity(store, "queue-turn", "task", "Queue 407 backlog")
    _record(store, activity.id, "The oldest job is queue-407-1.", status="succeeded")

    with psycopg.connect(context.database_url) as connection:
        rows = connection.execute(
            _test_sql("read_activity_memory_queue_messages.sql"),
            (activity.id,),
        ).fetchall()
    jobs = [row[0] for row in rows]

    assert len(jobs) == 1
    assert jobs[0]["terminal"] is True
    assert jobs[0]["event_watermark"] == max(
        event.sequence for event in store.list_events(activity.id)
    )


def test_activity_memory_worker_reuses_published_artifact_on_redelivery(
    lisa_test_context, monkeypatch,
):
    context = lisa_test_context()
    services = build_services(context)
    activity = _chat_activity(
        services.activity_store, "worker-turn", "task", "Queue 407 backlog"
    )
    _record(services.activity_store, activity.id, "The oldest job ID is queue-407-1.")
    job = services.conversation_checkpoint_queue.read_one()
    assert job is not None
    assert job["message"]["kind"] == "activity_memory"
    services.activity_store.update_activity_progress(
        activity.id,
        status="waiting",
        summary="A later event updates the task.",
        next_action="A later next action.",
    )
    llm = SimpleNamespace(model="test-memory-model")
    calls = []
    monkeypatch.setattr(
        conversation_checkpoints,
        "generate_checkpoint_summary",
        lambda payload, configured_llm, *, system_prompt: (
            calls.append((payload, configured_llm, system_prompt))
            or "The oldest job is queue-407-1."
        ),
    )
    worker = ConversationCheckpointWorker(
        services.conversation_checkpoint_queue,
        services.conversation_repository,
        lambda: llm,
        context,
        activities=services.activity_store,
    )

    worker._process(job)
    worker._process(job)

    configuration = activity_memory_configuration_fingerprint(llm.model)
    artifact = services.activity_store.get_activity_memory_artifact(
        activity.id, job["message"]["event_watermark"], configuration
    )
    assert artifact is not None
    assert artifact["summary"] == "The oldest job is queue-407-1."
    assert artifact["is_terminal"] is False
    assert len(calls) == 1
    assert "one Lisa activity" in calls[0][2]
    assert calls[0][0]["event_watermark"] == job["message"]["event_watermark"]
    assert calls[0][0]["next_action"] == "Continue the investigation"
    assert services.conversation_checkpoint_queue.read_one() is None
