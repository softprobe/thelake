"""Opt-in model-backed integration scenarios for Lisa's activity workflows."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from lisa.backends.openhands import OpenHandsBackend
from lisa.bootstrap import build_services
from lisa.repositories.conversations import FileConversationRepository
from lisa.runtime import confirmed_followup_id, confirmed_policy_proposal_id
from lisa.services.conversations import ConversationService

LIVE_ENABLED = os.getenv("LISA_RUN_ACTIVITY_MANAGEMENT_INTEGRATION") == "1"
HAS_MODEL_KEY = bool(
    os.getenv("LISA_BASE_URL")
    and os.getenv("LISA_API_KEY")
    or os.getenv("GOOGLE_API_KEY")
    or os.getenv("GEMINI_API_KEY")
)
pytestmark = pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_MODEL_KEY),
    reason=(
        "Activity integration scenarios call Lisa's configured model; set "
        "LISA_RUN_ACTIVITY_MANAGEMENT_INTEGRATION=1 and provide model credentials"
    ),
)


def _conversation_harness(lisa_test_context, monkeypatch, tmp_path):
    data_dir = tmp_path / "activity-integration"
    context = lisa_test_context(
        data_dir=data_dir,
        actor_id="activity-eval-owner",
        owner_user_ids=frozenset({"activity-eval-owner"}),
    )
    for name, value in {
        "LISA_WORKSPACE_ID": context.workspace,
        "LISA_DATA_DIR": str(data_dir),
        "LISA_DATABASE_URL": context.database_url,
        "LISA_DATABASE_READ_URL": context.database_read_url,
        "LISA_OWNER_USER_IDS": "activity-eval-owner",
        "LISA_AGENT_BACKEND": "openhands",
        "LISA_SKILL_DIR": str(Path(__file__).parents[1] / "skills"),
    }.items():
        monkeypatch.setenv(name, value)
    conversations = ConversationService(FileConversationRepository(data_dir))
    main = asyncio.run(conversations.main_conversation(context.workspace))
    return context, conversations, main["conversation_id"], build_services(context)


def _turn(conversations, context, conversation_id, text, *, source="web", channel=None):
    from lisa.runtime import (
        confirmed_policy_title,
        rejected_policy_proposal_id,
    )

    return asyncio.run(
        conversations.run_user_turn(
            conversation_id,
            context.workspace,
            text,
            run_turn=OpenHandsBackend().run_turn,
            confirmed_policy_title=confirmed_policy_title(text),
            confirmed_policy_proposal_id=confirmed_policy_proposal_id(text),
            rejected_policy_proposal_id=rejected_policy_proposal_id(text),
            confirmed_followup_id=confirmed_followup_id(text),
            turn_source=source,
            actor_id="activity-eval-owner",
            slack_channel_id=channel,
            slack_thread_ts="1780000000.000100" if channel else None,
        )
    )


def test_runtime_resumes_one_activity_from_a_separate_slack_thread(
    lisa_test_context, monkeypatch, tmp_path
):
    context, conversations, main_id, services = _conversation_harness(
        lisa_test_context, monkeypatch, tmp_path
    )
    _turn(
        conversations,
        context,
        main_id,
        "Track an unresolved multi-step investigation called 'Kestrel 742 retry audit'. "
        "The payment agent retried a timed-out charge twice; we do not yet know whether "
        "the idempotency key was preserved. Record this evidence and leave the activity "
        "open with the next action to check the retry key.",
    )
    before = services.activity_store.list_activity_feed(query="Kestrel 742", open_only=True)
    assert len(before) == 1, f"Expected one open Kestrel activity, got {before}"
    activity_id = str(before[0]["id"])

    slack_id = str(uuid4())
    conversations.create_conversation(context.workspace, slack_id)
    _turn(
        conversations,
        context,
        slack_id,
        "Continue the Kestrel 742 retry audit from its existing Lisa activity. New evidence: "
        "both attempts used the same idempotency key. Add this finding and update the next "
        "action to verify the payment provider's deduplication result. Do not create a second task.",
        source="slack",
        channel="C_ACTIVITY_EVAL",
    )
    after = services.activity_store.list_activity_feed(query="Kestrel 742", open_only=True)
    assert len(after) == 1, f"Lisa created a duplicate or lost the activity: {after}"
    assert str(after[0]["id"]) == activity_id
    detail = services.activity_store.get_activity_thread(activity_id)
    assert len(detail["actions"]) >= 2, (
        "The Slack continuation did not update the activity timeline"
    )
    assert after[0]["next_action"], "The resumed activity has no next action"


def test_runtime_proposes_policy_in_main_chat_and_applies_exact_slack_confirmation(
    lisa_test_context, monkeypatch, tmp_path
):
    context, conversations, main_id, services = _conversation_harness(
        lisa_test_context, monkeypatch, tmp_path
    )
    _turn(
        conversations,
        context,
        main_id,
        "We need a policy that requires approval before a refund over $500. Read the current "
        "workspace policy and prepare one named rule proposal. Show me the exact proposal ID; "
        "do not apply it yet.",
    )
    proposals = services.activity_store.list_policy_proposals()
    assert len(proposals) == 1 and proposals[0]["status"] == "pending", proposals
    assert services.policy_service.read_policy_records() == []

    slack_id = str(uuid4())
    conversations.create_conversation(context.workspace, slack_id)
    _turn(
        conversations,
        context,
        slack_id,
        f"Confirm policy proposal: {proposals[0]['id']}",
        source="slack",
        channel="C_ACTIVITY_EVAL",
    )
    saved = services.policy_service.read_policy_records()
    assert len(saved) == 1
    assert saved[0].approved is False
    assert "500" in saved[0].criteria
    assert services.activity_store.get_policy_proposal(str(proposals[0]["id"])) is None


def test_runtime_waits_for_exact_confirmation_before_activating_followup(
    lisa_test_context, monkeypatch, tmp_path
):
    context, conversations, main_id, services = _conversation_harness(
        lisa_test_context, monkeypatch, tmp_path
    )
    _turn(
        conversations,
        context,
        main_id,
        "Track a follow-up task called 'Kestrel 742 provider check'. Check the provider's "
        "deduplication result tomorrow at 10:00 UTC. Create a one-time follow-up suggestion "
        "for that time and show me its exact ID, but do not activate it.",
    )
    entries = services.activity_store.list_activity_feed(query="Kestrel 742 provider check")
    assert len(entries) == 1, entries
    activity_id = str(entries[0]["id"])
    followups = services.activity_store.list_activity_followups(activity_id)
    assert len(followups) == 1, followups
    followup = followups[0]
    assert followup["status"] == "pending", followup
    assert followup["due_at"] is not None
    assert followup["due_at"].utcoffset() is not None

    slack_id = str(uuid4())
    conversations.create_conversation(context.workspace, slack_id)
    _turn(
        conversations,
        context,
        slack_id,
        f"Confirm follow-up: {followup['id']}",
        source="slack",
        channel="C_ACTIVITY_EVAL",
    )
    confirmed = services.activity_store.list_activity_followups(activity_id)
    assert confirmed[0]["status"] == "approved", confirmed
    assert confirmed[0]["due_at"] <= datetime.now(UTC) + timedelta(days=2)
