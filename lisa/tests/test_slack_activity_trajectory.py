from __future__ import annotations

import asyncio

from lisa.bootstrap import build_services
from lisa.runtime import AgentTurn


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
                {"id": "slack-assistant-1", "role": "assistant", "text": "I will review it.", "sequence": 5},
            ],
            activities=[
                {"id": "slack-tool-1", "kind": "tool", "tool_name": "find_activity", "arguments": {}, "sequence": 2},
                {"id": "slack-result-1", "kind": "evidence", "tool_name": "find_activity", "detail": "Found the activity.", "sequence": 3},
                {"id": "slack-attach-1", "kind": "tool", "tool_name": "attach_chat_turn_to_activity", "arguments": {"activity_id": activity.id}, "sequence": 4},
            ],
            activity_id=activity.id,
        )

    asyncio.run(services.slack_event_service.process(
        path,
        run_turn=run_turn,
        confirm_policy_id=lambda _text: None,
        post_reply=lambda channel, thread, text: posted.append((channel, thread, text)),
    ))

    assert services.slack_event_service.events.load(path)["status"] == "complete"
    assert posted == [("C_ACTIVITY_TEST", "1781000000.000100", "I will review it.")]
    detail = services.activity_store.get_activity_thread(activity.id)
    assert [event["event_type"] for event in detail["actions"][-4:]] == [
        "chat.user", "runtime.tool", "runtime.result", "chat.assistant",
    ]
    linked = {(item["reference_type"], item["reference_id"]) for item in detail["references"]}
    assert {("message", "slack-user-1"), ("message", "slack-assistant-1")} <= linked
    assert {("runtime_event", "slack-tool-1"), ("runtime_event", "slack-result-1")} <= linked


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
            activity_id=activity.id,
        )

    asyncio.run(services.slack_event_service.process(
        path,
        run_turn=run_turn,
        confirm_policy_id=lambda _text: None,
        post_reply=lambda *_args: None,
    ))

    assert services.slack_event_service.events.load(path)["status"] == "complete"
    detail = services.activity_store.get_activity_thread(activity.id)
    assert detail["actions"] == before["actions"]
    assert detail["references"] == before["references"]
