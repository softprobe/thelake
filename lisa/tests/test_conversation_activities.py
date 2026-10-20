from __future__ import annotations

import asyncio
import json

from lisa.repositories.conversations import FileConversationRepository
from lisa.runtime import AgentTurn, selected_chat_activity_id
from lisa.services.conversations import ConversationService


class RecordingActivities:
    def __init__(self, known_ids=None):
        self.calls = []
        self.known_ids = known_ids

    def activity_exists(self, activity_id):
        return self.known_ids is None or activity_id in self.known_ids

    def record_conversation_trajectory(self, activity_id, messages, runtime_events):
        self.calls.append((
            activity_id,
            [dict(message) for message in messages],
            [dict(event) for event in runtime_events],
        ))


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
            activity_id="activity-1",
            messages=[
                {"id": "user-1", "role": "user", "text": "Update the refund rule.", "sequence": 1},
                {"id": "assistant-1", "role": "assistant", "text": "I updated the rule.", "sequence": 4},
            ],
            activities=[{"id": "tool-1", "kind": "tool", "tool_name": "write_policy", "sequence": 2}],
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
        [{"id": "tool-1", "kind": "tool", "tool_name": "write_policy", "sequence": 2}],
    )]


def test_activity_created_during_turn_is_selected_without_attach_tool():
    activity_id = "activity-1"
    events = [
        {
            "id": "create-call", "kind": "tool", "tool_name": "create_activity",
            "arguments": {"title": "Review refund policy"}, "sequence": 2,
        },
        {
            "id": "create-result", "kind": "evidence", "tool_name": "create_activity",
            "detail": json.dumps({"activity_id": activity_id}), "sequence": 3,
        },
    ]

    assert selected_chat_activity_id(events) == activity_id


def test_prefixed_openhands_tool_names_select_created_activity():
    events = [
        {
            "id": "create-result", "kind": "evidence",
            "tool_name": "mcp_lisa_create_activity",
            "detail": "[Tool 'create_activity' executed.]\n"
                      + json.dumps({"activity_id": "activity-1"}), "sequence": 2,
        },
        {
            "id": "attach", "kind": "tool",
            "tool_name": "mcp_lisa_attach_chat_turn_to_activity",
            "arguments": json.dumps({"activity_id": "activity-1"}), "sequence": 3,
        },
    ]

    assert selected_chat_activity_id(events) == "activity-1"


def test_prefixed_find_activity_list_selects_single_matching_result():
    events = [{
        "id": "find-result", "kind": "evidence", "tool_name": "find_activity",
        "detail": "[Tool 'find_activity' executed.]\n"
                 + json.dumps([{"id": "activity-1", "title": "Refund policy review"}]),
        "sequence": 2,
    }]

    assert selected_chat_activity_id(events) == "activity-1"


def test_find_activity_does_not_infer_from_multiple_or_malformed_results():
    multiple = [{
        "id": "find-result", "kind": "evidence", "tool_name": "find_activity",
        "detail": json.dumps([{"id": "activity-1"}, {"id": "activity-2"}]),
        "sequence": 2,
    }]
    malformed = [{
        "id": "find-result", "kind": "evidence", "tool_name": "find_activity",
        "detail": "[Tool 'find_activity' executed.]\n" + json.dumps([None]),
        "sequence": 2,
    }]

    assert selected_chat_activity_id(multiple) is None
    assert selected_chat_activity_id(malformed) is None


def test_blank_activity_ids_are_ignored():
    events = [
        {
            "id": "attach", "kind": "tool", "tool_name": "attach_chat_turn_to_activity",
            "arguments": json.dumps({"activity_id": "  "}), "sequence": 2,
        },
        {
            "id": "create-result", "kind": "evidence", "tool_name": "create_activity",
            "detail": "[Tool 'create_activity' executed.]\n"
                     + json.dumps({"activity_id": "  "}), "sequence": 3,
        },
    ]

    assert selected_chat_activity_id(events) is None


def test_activity_continuation_tool_selects_its_existing_activity():
    events = [
        {
            "id": "progress-call", "kind": "tool", "tool_name": "update_activity",
            "arguments": {"activity_id": "activity-1", "event_title": "Policy checked"},
            "sequence": 2,
        },
    ]

    assert selected_chat_activity_id(events) == "activity-1"


def test_multiple_activity_targets_require_explicit_selection():
    events = [
        {
            "id": "first", "kind": "tool", "tool_name": "update_activity",
            "arguments": {"activity_id": "activity-1"}, "sequence": 2,
        },
        {
            "id": "second", "kind": "tool", "tool_name": "complete_activity",
            "arguments": {"activity_id": "activity-2"}, "sequence": 3,
        },
    ]

    assert selected_chat_activity_id(events) is None


def test_created_and_continued_activity_targets_are_ambiguous():
    events = [
        {
            "id": "continued", "kind": "tool", "tool_name": "get_activity_thread",
            "arguments": {"activity_id": "existing"}, "sequence": 2,
        },
        {
            "id": "created", "kind": "evidence", "tool_name": "create_activity",
            "detail": json.dumps({"activity_id": "new"}), "sequence": 3,
        },
    ]

    assert selected_chat_activity_id(events) is None


def test_explicit_activity_selection_overrides_tool_inference():
    events = [
        {
            "id": "create-call", "kind": "tool", "tool_name": "create_activity",
            "arguments": {}, "sequence": 2,
        },
        {
            "id": "create-result", "kind": "evidence", "tool_name": "create_activity",
            "detail": json.dumps({"activity_id": "created-activity"}), "sequence": 3,
        },
        {
            "id": "attach", "kind": "tool", "tool_name": "attach_chat_turn_to_activity",
            "arguments": {"activity_id": "selected-activity"}, "sequence": 4,
        },
    ]

    assert selected_chat_activity_id(events) == "selected-activity"


def test_scheduled_turn_does_not_attach_internal_prompt_to_chat_activity(tmp_path):
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
            activity_id="activity-1",
            messages=[{"id": "scheduled-1", "role": "user", "text": "Internal prompt"}],
            activities=[],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Internal prompt", run_turn=run_turn,
        turn_source="scheduled",
    ))

    assert activities.calls == []


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
            activity_id="activity-1",
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
            activity_id="activity-no-longer-exists",
            messages=[{"id": "user-2", "role": "user", "text": "Continue."}],
            activities=[],
        )

    asyncio.run(service.run_user_turn(
        conversation_id, workspace_id, "Continue.", run_turn=run_turn,
        actor_id="owner-1",
    ))

    assert activities.calls == []
