from __future__ import annotations

import asyncio
from hashlib import sha256
from uuid import NAMESPACE_URL, UUID, uuid5

from lisa.bootstrap import build_services
from lisa.runtime import AgentTurn
from lisa.services.chat_activities import record_chat_activity_trajectories


def test_queued_slack_turn_persists_chat_and_runtime_events(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Review Slack agent behavior", source="slack",
    )
    receipt = {
        "workspace_id": context.workspace,
        "event_id": "Ev-activity-trajectory",
        "channel_id": "C_ACTIVITY_TEST",
        "thread_ts": "1781000000.000100",
        "actor_id": "activity-owner",
        "text": "Review this agent behavior.",
        "status": "pending",
    }
    services.slack_event_service.queue(receipt)
    path = services.slack_event_service.events.event_path(
        context.workspace, receipt["event_id"],
    )
    posted = []

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[
                {"id": "slack-user-1", "role": "user", "text": receipt["text"], "sequence": 1},
                {"id": "slack-assistant-1", "role": "assistant", "text": "I will review it.", "sequence": 7},
            ],
            activities=[
                {"id": "slack-resolve-1", "kind": "tool", "tool_name": "resolve_chat_activity", "arguments": {"disposition": "continue", "request_key": "review", "activity_id": activity.id}, "sequence": 2},
                {"id": "slack-resolve-result-1", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": f'{{"disposition":"continue","activity_id":"{activity.id}"}}', "sequence": 3},
                {"id": "slack-tool-1", "kind": "tool", "tool_name": "read_policy", "arguments": {}, "sequence": 4},
                {"id": "slack-result-1", "kind": "evidence", "tool_name": "read_policy", "detail": "Policy reviewed.", "sequence": 6},
            ],
        )

    asyncio.run(services.slack_event_service.process(
        path,
        run_turn=run_turn,
        post_reply=lambda channel, thread, text: posted.append((channel, thread, text)),
    ))

    assert services.slack_event_service.events.load(path)["status"] == "complete"
    assert posted == [("C_ACTIVITY_TEST", "1781000000.000100", "I will review it.")]
    detail = services.activity_store.get_activity_thread(activity.id)
    assert [event["event_type"] for event in detail["actions"][-6:]] == [
        "accepted", "chat.user", "runtime.tool", "runtime.result", "chat.assistant", "succeeded",
    ]
    assert detail["activity"]["status"] == "success"
    linked = {(item["reference_type"], item["reference_id"]) for item in detail["references"]}
    assert {("message", "slack-user-1"), ("message", "slack-assistant-1")} <= linked
    assert {("runtime_event", "slack-tool-1"), ("runtime_event", "slack-result-1")} <= linked
    assert not {
        ("runtime_event", "slack-resolve-1"),
        ("runtime_event", "slack-resolve-result-1"),
    } & linked


def test_slack_fallback_binds_current_message_after_historical_transcript(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    receipt = {
        "workspace_id": context.workspace, "event_id": "Ev-slack-fallback-current",
        "channel_id": "C_ACTIVITY_TEST", "thread_ts": "1781000000.000150",
        "actor_id": "activity-owner", "text": "Track the current invoice incident.",
        "status": "pending",
    }
    services.slack_event_service.queue(receipt)
    path = services.slack_event_service.events.event_path(context.workspace, receipt["event_id"])
    conversation_id = str(uuid5(
        NAMESPACE_URL,
        f"lisa:slack:{UUID(context.workspace)}:{receipt['channel_id']}:{receipt['thread_ts']}",
    ))
    directory = services.conversations.repository.conversation_directory(context.workspace, conversation_id)
    services.conversations.repository.save_metadata(directory, {
        "conversation_id": conversation_id,
        "workspace_id_hash": sha256(str(UUID(context.workspace)).encode()).hexdigest(),
        "messages": [{"id": "historical-user", "role": "user", "text": "Old request"}],
        "activities": [],
    })

    def run_turn(**_kwargs):
        return AgentTurn(messages=[
            {"id": "historical-user", "role": "user", "text": "Old request", "sequence": 1},
            {"id": "current-slack-user", "role": "user", "text": receipt["text"], "sequence": 2},
            {"id": "current-slack-assistant", "role": "assistant", "text": "I am tracking it.", "sequence": 4},
        ], activities=[])

    asyncio.run(services.slack_event_service.process(
        path, run_turn=run_turn,
        post_reply=lambda *_args: None,
    ))

    activity = services.activity_store.list_activity_feed(query="current invoice incident")[0]
    detail = services.activity_store.get_activity_thread(activity["id"])
    refs = {(item["reference_type"], item["reference_id"]) for item in detail["references"]}
    assert ("message", "current-slack-user") in refs
    assert ("message", "historical-user") not in refs
    assert any(
        action["event_type"] == "chat.user" and action["reference_id"] == "current-slack-user"
        for action in detail["actions"]
    )


def test_queued_slack_turn_does_not_persist_non_owner_chat(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Review Slack agent behavior", source="slack",
    )
    before = services.activity_store.get_activity_thread(activity.id)
    receipt = {
        "workspace_id": context.workspace,
        "event_id": "Ev-non-owner-activity-trajectory",
        "channel_id": "C_ACTIVITY_TEST",
        "thread_ts": "1781000000.000200",
        "actor_id": "workspace-member",
        "text": "Review this agent behavior.",
        "status": "pending",
    }
    services.slack_event_service.queue(receipt)
    path = services.slack_event_service.events.event_path(
        context.workspace, receipt["event_id"],
    )

    def run_turn(**_kwargs):
        return AgentTurn(
            messages=[
                {"id": "non-owner-user", "role": "user", "text": receipt["text"], "sequence": 1},
                {"id": "non-owner-assistant", "role": "assistant", "text": "Reviewed.", "sequence": 3},
            ],
            activities=[
                {
                    "id": "non-owner-create-result", "kind": "evidence",
                    "tool_name": "create_activity",
                    "detail": f'{{"activity_id":"{activity.id}"}}', "sequence": 2,
                },
            ],
        )

    asyncio.run(services.slack_event_service.process(
        path,
        run_turn=run_turn,
        post_reply=lambda *_args: None,
    ))

    assert services.slack_event_service.events.load(path)["status"] == "complete"
    detail = services.activity_store.get_activity_thread(activity.id)
    assert detail["actions"] == before["actions"]
    assert detail["references"] == before["references"]


def test_failed_policy_approval_keeps_previously_waiting_activity_open(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Approve policy P1.2", source="web",
    )

    record_chat_activity_trajectories(
        services.activity_store,
        activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "policy-resolve-create", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"create","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "policy-written", "kind": "evidence",
                "tool_name": "write_policy",
                "detail": '{"policy_id":"P1.2","approved":false}',
                "sequence": 2,
            },
        ],
        final_summary="Policy P1.2 is waiting for approval.",
    )
    waiting = services.activity_store.get_activity_thread(activity.id)["activity"]
    assert waiting["status"] == "blocked"
    assert waiting["next_action"] == "Approve policy: P1.2"
    record_chat_activity_trajectories(
        services.activity_store,
        activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "policy-resolve-approval", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"continue","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "approve-call", "kind": "tool", "tool_call_id": "approve-call",
                "tool_name": "approve_policy", "arguments": {"policy_id": "P1.2"},
                "sequence": 2,
            },
            {
                "id": "approve-error", "kind": "evidence", "tool_call_id": "approve-call",
                "tool_name": "approve_policy", "is_error": True,
                "detail": "Error calling tool 'approve_policy': owner identity unavailable",
                "sequence": 3,
            },
        ],
        final_summary="Lisa could not approve policy P1.2.",
    )

    current = services.activity_store.get_activity_thread(activity.id)["activity"]
    assert current["status"] == "blocked"
    assert current["next_action"] == "Approve policy: P1.2"


def test_missing_reply_keeps_previously_waiting_activity_open(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Approve policy P1.3", source="web",
    )
    record_chat_activity_trajectories(
        services.activity_store,
        activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "policy-resolve-create", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"create","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "policy-written", "kind": "evidence",
                "tool_name": "write_policy",
                "detail": '{"policy_id":"P1.3","approved":false}',
                "sequence": 2,
            },
        ],
        final_summary="Policy P1.3 is waiting for approval.",
    )

    record_chat_activity_trajectories(
        services.activity_store, activity_ids=[activity.id], messages=[],
        activities=[], final_summary="Lisa could not produce a final response.",
        turn_failed=True,
    )

    current = services.activity_store.get_activity_thread(activity.id)["activity"]
    assert current["status"] == "blocked"
    assert current["next_action"] == "Approve policy: P1.3"


def test_approving_one_of_two_policies_keeps_other_policy_waiting(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Update two agent policies", source="web",
    )
    record_chat_activity_trajectories(
        services.activity_store,
        activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "multi-policy-resolve", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"create","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "policy-two-written", "kind": "evidence",
                "tool_name": "write_policy",
                "detail": '{"policy_id":"P1.2","approved":false}',
                "sequence": 2,
            },
            {
                "id": "policy-three-written", "kind": "evidence",
                "tool_name": "write_policy",
                "detail": '{"policy_id":"P1.3","approved":false}',
                "sequence": 3,
            },
        ],
        final_summary="Policies P1.2 and P1.3 are waiting for approval.",
    )
    record_chat_activity_trajectories(
        services.activity_store,
        activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "multi-policy-resolve-approval", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"continue","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "policy-two-approved", "kind": "evidence",
                "tool_name": "approve_policy",
                "detail": '{"policy_id":"P1.2","approved":true}',
                "sequence": 2,
            },
        ],
        final_summary="Policy P1.2 has been approved.",
    )

    current = services.activity_store.get_activity_thread(activity.id)["activity"]
    assert current["status"] == "blocked"
    assert current["next_action"] == "Approve policy: P1.3"


def test_deferred_completion_is_scoped_to_its_activity(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    failed_activity = services.activity_store.create_activity(
        title="Investigate failed request", source="web",
    )
    succeeded_activity = services.activity_store.create_activity(
        title="Complete successful request", source="web",
    )

    record_chat_activity_trajectories(
        services.activity_store,
        activity_ids=[failed_activity.id, succeeded_activity.id], messages=[],
        activities=[
            {
                "id": "failed-resolve", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"continue","activity_id":"{failed_activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "succeeded-resolve", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"continue","activity_id":"{succeeded_activity.id}"}}',
                "sequence": 2,
            },
            {
                "id": "failed-complete-call", "kind": "tool",
                "tool_name": "complete_activity",
                "arguments": {"activity_id": failed_activity.id, "status": "error"},
                "sequence": 3,
            },
            {
                "id": "failed-complete-result", "kind": "evidence",
                "tool_name": "complete_activity",
                "detail": (
                    f'{{"activity_id":"{failed_activity.id}","status":"deferred",'
                    '"requested_status":"error","completed":false}'
                ),
                "sequence": 4,
            },
            {
                "id": "success-tool", "kind": "tool",
                "tool_name": "read_policy", "arguments": {}, "sequence": 5,
            },
            {
                "id": "success-result", "kind": "evidence",
                "tool_name": "read_policy", "detail": "Policy reviewed.", "sequence": 6,
            },
        ],
        final_summary="Lisa handled both requests.",
    )

    failed = services.activity_store.get_activity_thread(failed_activity.id)["activity"]
    succeeded = services.activity_store.get_activity_thread(succeeded_activity.id)["activity"]
    assert failed["status"] == "error"
    assert succeeded["status"] == "success"


def test_explicit_completion_resolves_a_previously_waiting_activity(lisa_test_context):
    context = lisa_test_context(owner_user_ids=frozenset({"activity-owner"}))
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Create Conversation greeting policy", source="web",
    )
    record_chat_activity_trajectories(
        services.activity_store, activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "policy-resolve", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"create","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "policy-created", "kind": "evidence",
                "tool_name": "write_policy",
                "detail": '{"policy_id":"P1.1","approved":false}',
                "sequence": 2,
            },
        ],
        final_summary="The policy is waiting for approval.",
    )

    record_chat_activity_trajectories(
        services.activity_store, activity_ids=[activity.id], messages=[],
        activities=[
            {
                "id": "completion-resolve", "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": f'{{"disposition":"continue","activity_id":"{activity.id}"}}',
                "sequence": 1,
            },
            {
                "id": "completion-call", "kind": "tool",
                "tool_name": "complete_activity",
                "arguments": {"activity_id": activity.id, "status": "success"},
                "sequence": 2,
            },
            {
                "id": "completion-result", "kind": "evidence",
                "tool_name": "complete_activity",
                "detail": (
                    f'{{"activity_id":"{activity.id}","status":"deferred",'
                    '"requested_status":"success","completed":false}'
                ),
                "sequence": 3,
            },
        ],
        final_summary="The owner confirms the requested activity is complete.",
    )

    current = services.activity_store.get_activity_thread(activity.id)["activity"]
    assert current["status"] == "success"
