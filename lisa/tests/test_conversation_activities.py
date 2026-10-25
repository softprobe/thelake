from __future__ import annotations

import asyncio
import json

import pytest

from lisa.repositories.conversations import FileConversationRepository
from lisa.runtime import AgentTurn, selected_chat_activity_ids
from lisa.services.conversations import ConversationService


class RecordingActivities:
    def __init__(self, known_ids=None):
        self.calls = []
        self.known_ids = known_ids
        self.completion_result = True
        self.final_status = "queued"

    def activity_exists(self, activity_id):
        return self.known_ids is None or activity_id in self.known_ids

    def record_conversation_trajectory(
        self, activity_id, messages, runtime_events, *, chat_turn_id=None,
    ):
        self.calls.append((
            activity_id,
            [dict(message) for message in messages],
            [dict(event) for event in runtime_events],
        ))

    def record_chat_turn_trajectory(
        self, activity_id, messages, runtime_events, *, summary, status,
        next_action=None, chat_turn_id=None,
    ):
        if status != "waiting" and not self.completion_result:
            raise RuntimeError(f"Lisa chat activity {activity_id} remains queued after turn finalization")
        self.record_conversation_trajectory(
            activity_id, messages, runtime_events, chat_turn_id=chat_turn_id,
        )
        self.finalized = (activity_id, summary, status, next_action)
        if status == "waiting":
            self.progress = (activity_id, {"status": status, "summary": summary, "next_action": next_action})
        else:
            self.completed = (activity_id, summary, status)
        return len(messages) + len(runtime_events)

    def complete_chat_activity(self, activity_id, summary, *, status="succeeded"):
        self.completed = (activity_id, summary, status)
        self.final_status = "success" if self.completion_result else "queued"
        return self.completion_result

    def get_activity_thread(self, activity_id):
        return {"activity": {"id": activity_id, "status": self.final_status}}

    def update_activity_progress(self, activity_id, **progress):
        self.progress = (activity_id, progress)

    def create_chat_activity(self, *, turn_id, title, summary, message_id=None, **_kwargs):
        activity_id = f"fallback-{turn_id}"
        self.known_ids = (self.known_ids or set()) | {activity_id}
        self.created = (activity_id, title, summary, message_id)
        return type("Activity", (), {"id": activity_id})()

    def fail_chat_turn(self, *args, **kwargs):
        self.failed = (args, kwargs)


def test_user_turn_records_chat_messages_on_the_selected_activity(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(
        FileConversationRepository(tmp_path), activities,
        owner_user_ids=frozenset({"owner-1"}),
    )
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[
                {"id": "user-1", "role": "user", "text": "Update the refund rule.", "sequence": 1},
                {"id": "assistant-1", "role": "assistant", "text": "I updated the rule.", "sequence": 4},
            ],
            activities=[
                {"id": "resolve-1", "kind": "tool", "tool_name": "resolve_chat_activity", "arguments": {"disposition": "continue", "activity_id": "activity-1", "request_key": "policy"}, "sequence": 2},
                {"id": "resolve-result-1", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": json.dumps({"activity_id": "activity-1", "disposition": "continue"}), "sequence": 3},
                {"id": "tool-1", "kind": "tool", "tool_name": "write_policy", "sequence": 4},
            ],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Update the refund rule.", run_turn=run_turn,
        actor_id="owner-1",
    ))

    assert activities.calls == [(
        "activity-1",
        [
            {"id": "user-1", "role": "user", "text": "Update the refund rule.", "sequence": 1},
            {"id": "assistant-1", "role": "assistant", "text": "I updated the rule.", "sequence": 4},
        ],
        [
            {"id": "tool-1", "kind": "tool", "tool_name": "write_policy", "sequence": 4},
        ],
    )]
    assert activities.completed == ("activity-1", "I updated the rule.", "succeeded")


def test_conversation_reload_preserves_more_than_one_hundred_tool_activities(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    service = ConversationService(FileConversationRepository(tmp_path))
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[{"id": "answer", "role": "assistant", "text": "Done.", "sequence": 241}],
            activities=[
                {"id": f"tool-{index}", "kind": "tool", "tool_name": "search_sessions", "sequence": index}
                for index in range(120)
            ],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Search many sessions.", run_turn=run_turn,
    ))

    reloaded = service.get_conversation(workspace_id, conversation_id)
    assert len(reloaded["activities"]) == 120
    assert reloaded["activities"][0]["id"] == "tool-0"
    assert reloaded["activities"][-1]["id"] == "tool-119"


def test_pending_policy_activity_remains_open_after_chat_reply(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(
        FileConversationRepository(tmp_path), activities,
        owner_user_ids=frozenset({"owner-1"}),
    )
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[
                {"id": "user-1", "role": "user", "text": "Add a policy."},
                {"id": "assistant-1", "role": "assistant", "text": "Policy P1.2 is pending approval."},
            ],
            activities=[
                {"id": "resolve", "kind": "evidence", "tool_name": "resolve_chat_activity",
                 "detail": '{"disposition":"create","activity_id":"activity-1"}', "sequence": 1},
                {"id": "write", "kind": "evidence", "tool_name": "write_policy",
                 "detail": '{"policy_id":"P1.2","approved":false}', "sequence": 2},
            ],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Add a policy.", run_turn=run_turn,
        actor_id="owner-1",
    ))

    assert not hasattr(activities, "completed")
    assert activities.progress == (
        "activity-1",
        {
            "status": "waiting",
            "summary": "Policy P1.2 is pending approval.",
            "next_action": "Approve policy: P1.2",
        },
    )


def test_failed_last_tool_result_closes_chat_activity_as_failed(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(
        FileConversationRepository(tmp_path), activities,
        owner_user_ids=frozenset({"owner-1"}),
    )
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[{"id": "assistant-1", "role": "assistant", "text": "The change failed."}],
            failed=True,
            activities=[
                {"id": "resolve", "kind": "evidence", "tool_name": "resolve_chat_activity",
                 "detail": '{"disposition":"create","activity_id":"activity-1"}', "sequence": 1},
                {"id": "error", "kind": "evidence", "tool_name": "delete_policy",
                 "detail": "[An error occurred during execution.] Error calling tool 'delete_policy': missing",
                 "sequence": 2},
            ],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Delete a policy.", run_turn=run_turn,
        actor_id="owner-1",
    ))

    assert activities.completed == ("activity-1", "The change failed.", "failed")


def test_unrelated_success_does_not_clear_failed_mutation():
    from lisa.runtime import chat_activity_tool_failed

    assert chat_activity_tool_failed([
        {"kind": "tool", "tool_name": "write_policy", "arguments": {"policy_id": "P1.2"},
         "tool_call_id": "write-1", "sequence": 1},
        {"kind": "evidence", "tool_name": "write_policy", "tool_call_id": "write-1", "sequence": 2,
         "is_error": True, "detail": "write failed"},
        {"kind": "tool", "tool_name": "read_policy", "arguments": {"policy_id": "P1.2"},
         "tool_call_id": "read-1", "sequence": 3},
        {"kind": "evidence", "tool_name": "read_policy", "tool_call_id": "read-1", "sequence": 4,
         "is_error": False, "detail": "current policy read successfully"},
    ])


def test_successful_retry_clears_same_tool_error():
    from lisa.runtime import chat_activity_tool_failed

    assert not chat_activity_tool_failed([
        {"kind": "tool", "tool_name": "write_policy", "arguments": {"policy_id": "P1.2"},
         "tool_call_id": "write-1", "sequence": 1},
        {"kind": "evidence", "tool_name": "write_policy", "tool_call_id": "write-1", "sequence": 2,
         "is_error": True, "detail": "write failed"},
        {"kind": "tool", "tool_name": "write_policy", "arguments": {"policy_id": "P1.2"},
         "tool_call_id": "write-2", "sequence": 3},
        {"kind": "evidence", "tool_name": "write_policy", "tool_call_id": "write-2", "sequence": 4,
         "is_error": False, "detail": "write succeeded"},
    ])


def test_nested_history_error_does_not_fail_read_activity():
    from lisa.runtime import chat_activity_tool_failed

    assert not chat_activity_tool_failed([
        {
            "kind": "tool", "tool_name": "get_activity_thread",
            "tool_call_id": "thread-1", "sequence": 1,
            "arguments": {"activity_id": "activity-1"},
        },
        {
            "kind": "evidence", "tool_name": "get_activity_thread",
            "tool_call_id": "thread-1", "sequence": 2, "is_error": False,
            "detail": (
                '[Tool \'get_activity_thread\' executed.] '
                '{"actions":[{"summary":"Error calling tool \'write_policy\': old failure"}]}'
            ),
        },
    ])


def test_waiting_activity_without_specific_next_action_stays_waiting():
    from lisa.runtime import chat_activity_waiting_state

    assert chat_activity_waiting_state([
        {"kind": "evidence", "tool_name": "set_activity_progress", "sequence": 1,
         "detail": '{"status":"waiting","updated":true}'},
    ]) == (True, "Continue the requested work")


def test_running_activity_with_next_action_remains_waiting_after_chat_turn():
    from lisa.runtime import chat_activity_waiting_state

    running_progress = [
        {
            "kind": "tool", "tool_name": "set_activity_progress", "sequence": 1,
            "arguments": {
                "activity_id": "activity-1", "status": "running",
                "summary": "The worker is idle while holding the migration lock.",
                "next_action": "Verify the worker can be safely stopped",
            },
        },
        {
            "kind": "evidence", "tool_name": "set_activity_progress", "sequence": 2,
            "detail": '{"activity_id":"activity-1","status":"running","updated":true}',
        },
    ]
    waiting_progress = [
        {
            "kind": "tool", "tool_name": "set_activity_progress", "sequence": 1,
            "arguments": {
                "activity_id": "activity-1", "status": "waiting",
                "summary": "The investigation is waiting on the owner.",
                "next_action": "Ask the owner to confirm the ledger entry",
            },
        },
        {
            "kind": "evidence", "tool_name": "set_activity_progress", "sequence": 2,
            "detail": '{"activity_id":"activity-1","status":"waiting","updated":true}',
        },
    ]
    assert chat_activity_waiting_state(running_progress) == (
        True, "Verify the worker can be safely stopped"
    )
    assert chat_activity_waiting_state(waiting_progress) == (
        True, "Ask the owner to confirm the ledger entry"
    )


def test_queued_activity_with_next_action_remains_open_after_chat_turn():
    from lisa.runtime import chat_activity_waiting_state

    queued_progress = [
        {
            "kind": "tool", "tool_name": "set_activity_progress", "sequence": 1,
            "arguments": {
                "activity_id": "activity-1", "status": "queued",
                "summary": "The owner asked Lisa to track a future investigation.",
                "next_action": "Inspect the worker trace and request ID",
            },
        },
        {
            "kind": "evidence", "tool_name": "set_activity_progress", "sequence": 2,
            "detail": '{"activity_id":"activity-1","status":"queued","updated":true}',
        },
    ]

    assert chat_activity_waiting_state(queued_progress) == (
        True, "Inspect the worker trace and request ID"
    )


def test_finalized_evaluation_result_overrides_an_older_waiting_progress():
    from lisa.runtime import chat_activity_requested_status

    assert chat_activity_requested_status([
        {
            "kind": "evidence", "tool_name": "evaluate_session", "sequence": 1,
            "detail": (
                '[Tool \'evaluate_session\' executed.] '
                '{"status":"error","activity_id":"activity-1",'
                '"activity_finalized":true}'
            ),
        },
    ]) == "failed"


def test_retryable_chat_evaluation_keeps_activity_open_with_a_retry_action():
    from lisa.runtime import chat_activity_requested_status, chat_activity_waiting_state

    events = [{
        "kind": "evidence",
        "tool_name": "evaluate_session",
        "sequence": 1,
        "detail": json.dumps({
            "status": "evaluation_error",
            "activity_id": "activity-1",
            "activity_finalized": False,
        }),
    }]

    assert chat_activity_requested_status(events) is None
    assert chat_activity_waiting_state(events) == (
        True, "Retry the incomplete session evaluation"
    )


def test_evaluation_owned_activity_finalization_only_records_chat_trajectory():
    from lisa.services.chat_activities import record_chat_activity_trajectories

    activities = RecordingActivities(known_ids={"activity-1"})
    runtime_events = [
        {
            "id": "resolve-result", "kind": "evidence",
            "tool_name": "resolve_chat_activity", "sequence": 1,
            "detail": '{"disposition":"continue","activity_id":"activity-1"}',
        },
        {
            "id": "evaluation-result", "kind": "evidence",
            "tool_name": "evaluate_session", "sequence": 2,
        "detail": (
            '[Tool \'evaluate_session\' executed.] '
            '{"status":"error","activity_id":"activity-1",'
            '"activity_finalized":true}'
        ),
        },
    ]

    record_chat_activity_trajectories(
        activities,
        activity_ids=["activity-1"],
        messages=[{"id": "assistant-1", "role": "assistant", "text": "Done."}],
        activities=runtime_events,
        final_summary="The review failed because the session is missing.",
    )

    assert activities.calls == [("activity-1", [
        {"id": "assistant-1", "role": "assistant", "text": "Done."},
    ], [runtime_events[1]])]
    assert not hasattr(activities, "finalized")


def test_approving_one_policy_does_not_clear_another_pending_policy():
    from lisa.runtime import chat_activity_waiting_state

    assert chat_activity_waiting_state([
        {"kind": "evidence", "tool_name": "write_policy", "sequence": 1,
         "detail": '{"policy_id":"P1.2","approved":false}'},
        {"kind": "evidence", "tool_name": "write_policy", "sequence": 2,
         "detail": '{"policy_id":"P1.3","approved":false}'},
        {"kind": "evidence", "tool_name": "approve_policy", "sequence": 3,
         "detail": '{"policy_id":"P1.2","approved":true}'},
        {"kind": "evidence", "tool_name": "complete_activity", "sequence": 4,
         "detail": '{"completed":true,"status":"success"}'},
    ]) == (True, "Approve policy: P1.3")


def test_rejected_chat_completion_is_reported(caplog):
    from lisa.services.chat_activities import record_chat_activity_trajectories

    activities = RecordingActivities(known_ids={"activity-1"})
    activities.completion_result = False
    with pytest.raises(RuntimeError, match="remains queued"):
        record_chat_activity_trajectories(
            activities,
            activity_ids=["activity-1"],
            messages=[],
            activities=[],
            final_summary="Lisa replied.",
        )

    assert "Could not persist and finalize Lisa chat activity activity-1" in caplog.text


def test_chat_finalization_errors_propagate_to_the_turn_boundary():
    from lisa.services.chat_activities import record_chat_activity_trajectories

    activities = RecordingActivities(known_ids={"activity-1"})

    def fail_completion(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    activities.record_chat_turn_trajectory = fail_completion
    with pytest.raises(RuntimeError, match="database unavailable"):
        record_chat_activity_trajectories(
            activities,
            activity_ids=["activity-1"],
            messages=[],
            activities=[],
            final_summary="Lisa replied.",
        )


def test_missing_resolved_activity_is_a_tracking_failure():
    from lisa.services.chat_activities import record_chat_activity_trajectories

    activities = RecordingActivities(known_ids=set())
    with pytest.raises(RuntimeError, match="does not exist"):
        record_chat_activity_trajectories(
            activities,
            activity_ids=["deleted-activity"],
            messages=[],
            activities=[],
            final_summary="Lisa replied.",
        )


def test_resolved_chat_activities_collects_multiple_dispositions():
    events = [
        {
            "id": "first", "kind": "tool", "tool_name": "resolve_chat_activity",
            "arguments": {"disposition": "continue", "activity_id": "activity-1", "request_key": "refund"}, "sequence": 2,
        },
        {
            "id": "first-result", "kind": "evidence", "tool_name": "resolve_chat_activity",
            "detail": json.dumps({"disposition": "continue", "activity_id": "activity-1"}), "sequence": 3,
        },
        {
            "id": "second-result", "kind": "evidence", "tool_name": "mcp_lisa_resolve_chat_activity",
            "detail": "[Tool 'resolve_chat_activity' executed.]\n" + json.dumps({"disposition": "create", "activity_id": "activity-2"}), "sequence": 4,
        },
    ]

    assert selected_chat_activity_ids(events) == ["activity-1", "activity-2"]


def test_resolved_activity_accepts_prefixed_names_and_json_arguments():
    events = [
        {"id": "result", "kind": "evidence", "tool_name": "mcp_lisa_resolve_chat_activity",
         "detail": "[Tool 'resolve_chat_activity' executed.]\n" + json.dumps({"disposition": "continue", "activity_id": "activity-1"}), "sequence": 2},
    ]

    assert selected_chat_activity_ids(events) == ["activity-1"]


def test_activity_runtime_events_are_partitioned_by_resolved_request():
    from lisa.runtime import activity_events_for_target

    events = [
        {"id": "resolve-a", "kind": "tool", "tool_name": "resolve_chat_activity", "sequence": 1},
        {"id": "result-a", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": '{"activity_id":"a","disposition":"create"}', "sequence": 2},
        {"id": "tool-a", "kind": "tool", "tool_name": "read_policy", "sequence": 3},
        {"id": "resolve-b", "kind": "tool", "tool_name": "resolve_chat_activity", "sequence": 4},
        {"id": "result-b", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": '{"activity_id":"b","disposition":"create"}', "sequence": 5},
        {"id": "tool-b", "kind": "tool", "tool_name": "evaluate_session", "sequence": 6},
    ]

    assert [item["id"] for item in activity_events_for_target(events, ["a", "b"], "a")] == ["tool-a"]
    assert [item["id"] for item in activity_events_for_target(events, ["a", "b"], "b")] == ["tool-b"]


def test_activity_context_lookups_are_not_copied_into_the_next_activity():
    from lisa.runtime import activity_events_for_target

    events = [
        {
            "id": "find-a", "kind": "tool", "tool_name": "find_activity",
            "tool_call_id": "find-a-call", "sequence": 1,
        },
        {
            "id": "find-a-result", "kind": "evidence", "tool_name": "find_activity",
            "tool_call_id": "find-a-call",
            "detail": '[{"id":"activity-a","title":"Orchid"},{"id":"activity-b","title":"Maple"}]',
            "sequence": 2,
        },
        {
            "id": "thread-a", "kind": "tool", "tool_name": "get_activity_thread",
            "tool_call_id": "thread-a-call", "arguments": {"activity_id": "activity-a"},
            "sequence": 3,
        },
        {
            "id": "thread-a-result", "kind": "evidence", "tool_name": "get_activity_thread",
            "tool_call_id": "thread-a-call", "detail": "Orchid-only history: ORCHID-SECRET",
            "sequence": 4,
        },
        {
            "id": "resolve-b", "kind": "tool", "tool_name": "resolve_chat_activity",
            "sequence": 5,
        },
        {
            "id": "resolve-b-result", "kind": "evidence",
            "tool_name": "resolve_chat_activity",
            "detail": '{"activity_id":"activity-b","disposition":"continue"}',
            "sequence": 6,
        },
        {
            "id": "memory-b", "kind": "tool", "tool_name": "load_activity_memory",
            "arguments": {"activity_id": "activity-b"}, "sequence": 7,
        },
        {
            "id": "memory-b-result", "kind": "evidence",
            "tool_name": "load_activity_memory",
            "detail": "Generated task summary and source references",
            "sequence": 8,
        },
        {
            "id": "write-b", "kind": "tool", "tool_name": "set_activity_progress",
            "arguments": {"activity_id": "activity-b"}, "sequence": 9,
        },
    ]

    target_b = activity_events_for_target(events, ["activity-b"], "activity-b")
    assert [item["id"] for item in target_b] == ["write-b"]


def test_mixed_resolver_payload_is_not_persisted_as_activity_evidence():
    from lisa.runtime import activity_events_for_target

    events = [
        {
            "id": "resolve", "kind": "tool", "tool_name": "resolve_chat_activity",
            "arguments": {
                "activity_id": "activity-b",
                "summary": "Maple work uses MAPLE-SECRET; Orchid uses ORCHID-SECRET",
            },
            "sequence": 1,
        },
        {
            "id": "resolve-result", "kind": "evidence",
            "tool_name": "resolve_chat_activity",
            "detail": json.dumps({
                "activity_id": "activity-b",
                "disposition": "continue",
                "summary": "Maple's fact is MAPLE-SECRET; Orchid's fact is ORCHID-SECRET",
            }),
            "sequence": 2,
        },
        {
            "id": "write-b", "kind": "tool", "tool_name": "set_activity_progress",
            "arguments": {"activity_id": "activity-b", "summary": "Maple update"},
            "sequence": 3,
        },
    ]

    target_b = activity_events_for_target(events, ["activity-b"], "activity-b")

    assert [item["id"] for item in target_b] == ["write-b"]
    assert "ORCHID-SECRET" not in json.dumps(target_b)


def test_explicit_skip_is_distinct_from_missing_disposition():
    events = [{"id": "skip", "kind": "tool", "tool_name": "skip_chat_activity_for_turn",
               "arguments": {"reason": "Greeting"}, "sequence": 2}]

    assert selected_chat_activity_ids(events) == []


def test_non_owner_turn_does_not_copy_chat_into_shared_activity(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(
        FileConversationRepository(tmp_path), activities,
        owner_user_ids=frozenset({"owner-1"}),
    )
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[{"id": "user-1", "role": "user", "text": "Private chat"}],
            activities=[],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Private chat", run_turn=run_turn,
        actor_id="outsider",
    ))

    assert activities.calls == []


def test_missing_activity_target_does_not_break_chat_turn(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities(known_ids=set())
    service = ConversationService(
        FileConversationRepository(tmp_path), activities,
        owner_user_ids=frozenset({"owner-1"}),
    )
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[{"id": "user-2", "role": "user", "text": "Continue."}],
            failed=True,
            activities=[{"id": "work", "kind": "tool", "tool_name": "read_policy", "sequence": 1}],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Continue.", run_turn=run_turn,
        actor_id="owner-1",
    ))

    assert len(activities.calls) == 1
    assert activities.calls[0][0].startswith("fallback-")
    assert activities.calls[0][2][0]["label"] == (
        "Activity routing fallback: Lisa provided no valid resolution for this request"
    )
    assert activities.completed[2] == "failed"
    assert activities.completed == (
        activities.created[0],
        "Lisa finished this request; see the timeline for recorded tool results.",
        "failed",
    )
    assert [event["id"] for event in activities.calls[0][2]] == [
        f"fallback-resolution:{activities.created[0].removeprefix('fallback-')}", "work",
    ]


def test_fallback_binds_the_current_user_message_after_historical_messages(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(
        FileConversationRepository(tmp_path), activities,
        owner_user_ids=frozenset({"owner-1"}),
    )
    service.create_conversation(workspace_id, conversation_id)

    def previous_turn(**_kwargs):
        return AgentTurn(messages=[
            {"id": "old-user", "role": "user", "text": "Old request."},
            {"id": "old-assistant", "role": "assistant", "text": "Old reply."},
        ], activities=[])

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Old request.", run_turn=previous_turn,
        actor_id="someone-else",
    ))

    def current_turn(**_kwargs):
        return AgentTurn(messages=[
            {"id": "old-user", "role": "user", "text": "Old request."},
            {"id": "old-assistant", "role": "assistant", "text": "Old reply."},
            {"id": "current-user", "role": "user", "text": "Track this request."},
            {"id": "current-assistant", "role": "assistant", "text": "I tracked it."},
        ], activities=[])

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Track this request.", run_turn=current_turn,
        actor_id="owner-1",
    ))

    assert activities.created[3] == "current-user"
    assert [message["id"] for message in activities.calls[0][1]] == [
        "current-user", "current-assistant",
    ]


def test_independent_requests_share_chat_but_keep_tool_events_scoped(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(FileConversationRepository(tmp_path), activities,
                                  owner_user_ids=frozenset({"owner-1"}))
    service.create_conversation(workspace_id, conversation_id)

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[{"id": "user", "role": "user", "text": "Check policy and session."}],
            activities=[
                {"id": "r1", "kind": "tool", "tool_name": "resolve_chat_activity", "arguments": {"request_key": "policy"}, "sequence": 1},
                {"id": "rr1", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": '{"disposition":"create","activity_id":"a1"}', "sequence": 2},
                {"id": "t1", "kind": "tool", "tool_name": "read_policy", "sequence": 3},
                {"id": "r2", "kind": "tool", "tool_name": "resolve_chat_activity", "arguments": {"request_key": "session"}, "sequence": 4},
                {"id": "rr2", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": '{"disposition":"create","activity_id":"a2"}', "sequence": 5},
                {"id": "t2", "kind": "tool", "tool_name": "evaluate_session", "sequence": 6},
            ],
        )

    asyncio.run(service.run_user_turn(conversation_id, workspace_id, "Check policy and session.",
                                      run_turn=run_turn, actor_id="owner-1"))
    assert [call[0] for call in activities.calls] == ["a1", "a2"]
    assert all([message["id"] for message in call[1]] == ["user"] for call in activities.calls)
    assert [event["id"] for event in activities.calls[0][2]] == ["t1"]
    assert [event["id"] for event in activities.calls[1][2]] == ["t2"]


def test_explicit_skip_creates_no_fallback_activity(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(FileConversationRepository(tmp_path), activities,
                                  owner_user_ids=frozenset({"owner-1"}))
    service.create_conversation(workspace_id, conversation_id)
    run_turn = lambda **_kwargs: AgentTurn(messages=[], activities=[
        {"id": "skip", "kind": "evidence", "tool_name": "skip_chat_activity_for_turn",
         "detail": '{"skipped":true}', "sequence": 1},
    ])
    asyncio.run(service.run_user_turn(conversation_id, workspace_id, "Hello.", run_turn=run_turn,
                                      actor_id="owner-1"))
    assert activities.calls == []


def test_failure_before_resolution_creates_visible_failed_fallback(tmp_path):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    activities = RecordingActivities()
    service = ConversationService(FileConversationRepository(tmp_path), activities,
                                  owner_user_ids=frozenset({"owner-1"}))
    service.create_conversation(workspace_id, conversation_id)
    def fail(**_kwargs):
        raise RuntimeError("backend unavailable")
    with pytest.raises(Exception):
        asyncio.run(service.run_user_turn(conversation_id, workspace_id, "Do work.",
                                          run_turn=fail, actor_id="owner-1"))
    assert activities.failed[0][1] == "Do work."
    assert activities.failed[1]["source"] == "web"
