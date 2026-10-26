"""Opt-in OpenHands runtime check for the packaged policy discovery tools."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from lisa.backends.openhands import OpenHandsBackend
from lisa.bootstrap import build_services
from lisa.models.policy_discovery import PolicySuggestionDraft
from lisa.repositories.conversations import FileConversationRepository
from lisa.services.conversations import ConversationService

LIVE_ENABLED = os.getenv("LISA_RUN_POLICY_DISCOVERY_INTEGRATION") == "1"
HAS_MODEL_KEY = bool(
    (os.getenv("LISA_BASE_URL") and os.getenv("LISA_API_KEY"))
    or os.getenv("GOOGLE_API_KEY")
    or os.getenv("GEMINI_API_KEY")
)
pytestmark = pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_MODEL_KEY),
    reason=(
        "Policy discovery integration calls Lisa's configured model; set "
        "LISA_RUN_POLICY_DISCOVERY_INTEGRATION=1 and provide model credentials"
    ),
)


def test_runtime_reads_evidence_backed_suggestion_without_approving(
    lisa_test_context, monkeypatch, tmp_path
):
    data_dir = tmp_path / "policy-discovery-integration"
    context = lisa_test_context(
        data_dir=data_dir,
        actor_id="policy-discovery-owner",
        owner_user_ids=frozenset({"policy-discovery-owner"}),
    )
    for name, value in {
        "LISA_WORKSPACE_ID": context.workspace,
        "LISA_DATA_DIR": str(data_dir),
        "LISA_DATABASE_URL": context.database_url,
        "LISA_DATABASE_READ_URL": context.database_read_url,
        "LISA_OWNER_USER_IDS": "policy-discovery-owner",
        "LISA_AGENT_BACKEND": "openhands",
        "LISA_SKILL_DIR": str(Path(__file__).parents[1] / "skills"),
    }.items():
        monkeypatch.setenv(name, value)

    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Policy discovery source evidence",
        source="session_push",
        kind="session_evaluation",
    )
    signal_id = services.policy_discovery.repository.save_signal(
        session_id="synthetic-deletion-session",
        activity_id=activity.id,
        agent_name="account-agent",
        summary="A destructive account deletion followed a request without confirmation.",
        span_ids=["user-span-17", "tool-span-18"],
        evidence=[
            {"span_id": "user-span-17", "kind": "user_message", "content": "Delete my account."},
            {"span_id": "tool-span-18", "kind": "tool_call", "name": "delete_account", "content": "Deleted account."},
        ],
    )
    draft = PolicySuggestionDraft.model_validate({
        "name": "Confirm irreversible account actions",
        "criteria": "Before deleting an account, explain the consequence and obtain explicit confirmation.",
        "scope": "agent",
        "target_agent_name": "account-agent",
        "rationale": "The cited trace shows an irreversible action without a confirmation step.",
        "signal_ids": [signal_id],
    })
    suggestion = services.policy_discovery.repository.create_suggestions(
        drafts=[draft],
        signal_rows=services.policy_discovery.repository.pending_signals(),
        overlaps_by_draft=[[]],
    )[0]

    conversations = ConversationService(
        FileConversationRepository(data_dir),
        activity_store=services.activity_store,
        owner_user_ids=context.owner_user_ids,
    )
    main = asyncio.run(conversations.main_conversation(context.workspace))
    response = asyncio.run(conversations.run_user_turn(
        main["conversation_id"],
        context.workspace,
        f"Invoke the learn-agent-policy skill, then use its policy suggestion tools "
        f"to inspect suggestion {suggestion['id']}. "
        "Read its cited user and tool evidence, summarize whether it is supported, "
        "and do not approve or modify it.",
        run_turn=OpenHandsBackend().run_turn,
        turn_source="web",
        actor_id="policy-discovery-owner",
    ))

    transcript = "\n".join(
        str(item.get("text", "")) for item in response["messages"]
        if item.get("role") == "assistant"
    )
    assert suggestion["id"] in transcript
    assert "delete_account" in transcript
    persisted = services.policy_discovery.get_suggestion(suggestion["id"])
    assert persisted["status"] == "pending"
    assert services.policy_service.read_policy_records("agent", "account-agent") == []
    runtime_record = str(response["activities"])
    assert "invoke_skill" in runtime_record
    assert "learn-agent-policy" in runtime_record
    assert "read_policy_suggestion" in runtime_record
    assert "load_tool_namespace" in runtime_record
