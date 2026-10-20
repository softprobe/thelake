"""Integrity and deterministic scoring contracts for the agent benchmark dataset."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmark.activity_runner import (
    _interpolate,
    _rate,
    _score_state,
    _score_trajectory_links,
    _score_turn,
    _update_substitutions,
)
from datasets.activity_dataset import ActivityLabels, ActivityTurn, load_activity_dataset
from lisa.backends.openhands import LISA_SYSTEM_PROMPT
from lisa.views.database import LISA_DATABASE_SCHEMA

DATASET = Path(__file__).resolve().parents[1] / "datasets/activity_management"


def test_activity_benchmark_release_has_frozen_cases_and_both_splits():
    manifest, cases, digest = load_activity_dataset(DATASET / "manifests/benchmark.json")

    assert manifest.case_count == len(cases) == 17
    assert digest
    assert {case.split for case in cases} == {"development", "benchmark"}
    assert {case.category for case in cases} >= {
        "create",
        "update",
        "continuity",
        "policy",
        "followup",
        "evaluation",
    }
    assert len({case.case_id for case in cases}) == len(cases)
    history_cases = [case for case in cases if case.category == "evaluation"]
    assert len(history_cases) == 3
    assert all(case.historical_evaluations for case in history_cases)


def test_activity_benchmark_covers_policy_creation_approval_and_in_place_update():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    case = next(case for case in cases if case.case_id == "create_approve_and_update_policy_in_place")

    assert case.category == "policy"
    assert case.split == "benchmark"
    assert [turn.require_tools for turn in case.turns] == [
        ["write_policy", "attach_chat_turn_to_activity"],
        ["approve_policy", "attach_chat_turn_to_activity"],
        ["write_policy", "attach_chat_turn_to_activity"],
    ]
    assert "Approve policy: {policy_id}" == case.turns[1].text
    assert "{policy_id}" in case.turns[2].text
    assert case.turns[2].tool_arguments_must_include == ["P1.1", "Refund Approval Guard"]
    assert case.turns[2].reply_must_include == ["P1.1", "approved: no"]
    assert case.expected.policy_count == 1
    assert case.expected.pending_policy_count == 1
    assert case.expected.activity_count == 1


def test_activity_benchmark_deletes_existing_policy_and_checks_database_absence():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    case = next(case for case in cases if case.case_id == "delete_existing_policy")

    assert case.category == "policy"
    assert case.split == "benchmark"
    assert [turn.require_tools for turn in case.turns] == [
        ["write_policy", "attach_chat_turn_to_activity"],
        ["delete_policy", "attach_chat_turn_to_activity"],
    ]
    assert case.turns[1].text == "Delete policy: {policy_id}"
    assert case.expected.activity_count == 2
    assert case.expected.policy_count == 0
    assert case.expected.deleted_policy_ids == ["$policy_id"]


def test_activity_benchmark_rejects_changed_case_bytes(tmp_path):
    copied = tmp_path / "activity_management"
    (copied / "manifests").mkdir(parents=True)
    (copied / "cases.jsonl").write_text((DATASET / "cases.jsonl").read_text() + "\n")
    (copied / "manifests/benchmark.json").write_text(
        (DATASET / "manifests/benchmark.json").read_text()
    )

    with pytest.raises(ValueError, match="digest mismatch"):
        load_activity_dataset(copied / "manifests/benchmark.json")


def test_activity_benchmark_score_checks_real_policy_approval_trace_and_reply_rubric():
    turn = ActivityTurn(
        chat="slack", text="Approve policy: {policy_id}",
        require_tools=["approve_policy"], forbid_tools=["write_policy"],
        reply_must_include=["$policy_id", "refund"],
        reply_must_not_include=["I already changed it"],
    )
    result = {
        "activities": [{"kind": "tool", "tool_name": "approve_policy"}],
        "messages": [{"role": "assistant", "text": "Approved P1.1 for the refund rule."}],
    }
    scored = _score_turn(turn, result, {"policy_id": "P1.1"}, 1)
    assert all(item["passed"] for item in scored["tool_checks"] + scored["communication_checks"])
    assert _interpolate("Approve policy: {policy_id}", {"policy_id": "P1.1"}) == "Approve policy: P1.1"


def test_activity_benchmark_interpolates_dynamic_policy_ids_in_tool_arguments():
    turn = ActivityTurn(
        chat="slack", text="Delete policy: {policy_id}",
        require_tools=["delete_policy", "attach_chat_turn_to_activity"],
        tool_arguments_must_include=["$policy_id"],
    )
    result = {
        "activities": [
            {"kind": "tool", "tool_name": "delete_policy", "arguments": {"policy_id": "P1.1"}},
            {"kind": "tool", "tool_name": "attach_chat_turn_to_activity", "arguments": {"activity_id": "activity-1"}},
        ],
        "messages": [],
    }

    scored = _score_turn(turn, result, {"policy_id": "P1.1"}, 1)

    assert all(item["passed"] for item in scored["tool_checks"])

def test_activity_benchmark_checks_database_tool_arguments():
    turn = ActivityTurn(
        chat="main",
        text="Find a historical result",
        require_tools=["lisa_db"],
        tool_arguments_must_include=["readable_evaluations", "session_id", "session-1"],
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "lisa_db",
                "arguments": {"query": "SELECT verdict FROM readable_evaluations WHERE session_id='session-1'"},
            }
        ],
        "messages": [{"role": "assistant", "text": "The result is fail."}],
    }
    scored = _score_turn(turn, result, {}, 1)
    assert all(check["passed"] for check in scored["tool_checks"])

    result["activities"][0]["arguments"]["query"] = "SELECT verdict FROM readable_evaluations"
    scored = _score_turn(turn, result, {}, 1)
    assert not next(
        check
        for check in scored["tool_checks"]
        if check["check"] == "tool_arguments_contain:session-1"
    )["passed"]


def test_activity_benchmark_requires_session_id_in_sql_filter():
    turn = ActivityTurn(
        chat="main",
        text="Find a historical result",
        tool_queries_must_filter_sessions=["session-1"],
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "lisa_db",
                "arguments": "SELECT verdict FROM readable_evaluations WHERE session_id = 'session-1'",
            }
        ],
        "messages": [],
    }
    scored = _score_turn(turn, result, {}, 1)
    assert any(check["passed"] for check in scored["tool_checks"] if check["check"] == "session_filter:session-1")

    result["activities"][0]["arguments"] = "SELECT verdict FROM readable_evaluations -- session-1"
    scored = _score_turn(turn, result, {}, 1)
    assert not next(
        check for check in scored["tool_checks"] if check["check"] == "session_filter:session-1"
    )["passed"]


def test_activity_benchmark_checks_tool_order_and_exact_call_count():
    turn = ActivityTurn(
        chat="main",
        text="Resume the existing work",
        tool_order=["find_activity", "set_activity_progress"],
        tool_call_counts={"create_activity": 0, "find_activity": 1},
    )
    result = {
        "activities": [
            {"kind": "tool", "tool_name": "find_activity"},
            {"kind": "tool", "tool_name": "get_activity_thread"},
            {"kind": "tool", "tool_name": "set_activity_progress"},
        ],
        "messages": [],
    }

    scored = _score_turn(turn, result, {}, 0)

    assert all(check["passed"] for check in scored["tool_checks"])
    result["activities"].append({"kind": "tool", "tool_name": "create_activity"})
    scored = _score_turn(turn, result, {}, 0)
    assert not next(
        check
        for check in scored["tool_checks"]
        if check["check"] == "tool_call_count:create_activity"
    )["passed"]


def test_activity_benchmark_requires_selected_chat_and_runtime_events_to_be_linked():
    class ActivityStore:
        def __init__(self, references):
            self.references = references

        def get_activity_thread(self, activity_id):
            assert activity_id == "activity-1"
            return {"references": self.references}

    result = {
        "messages": [
            {"id": "old-user", "role": "user"},
            {"id": "new-user", "role": "user"},
            {"id": "new-assistant", "role": "assistant"},
        ],
        "activities": [
            {"id": "tool-1", "tool_name": "write_policy"},
            {"id": "attach-1", "tool_name": "attach_chat_turn_to_activity", "arguments": {"activity_id": "activity-1"}},
        ],
    }
    linked = [
        {"reference_type": "message", "reference_id": "new-user"},
        {"reference_type": "message", "reference_id": "new-assistant"},
        {"reference_type": "runtime_event", "reference_id": "tool-1"},
    ]
    assert _score_trajectory_links(ActivityStore(linked), result, {"old-user"})["passed"]
    missing = _score_trajectory_links(ActivityStore(linked[:2]), result, {"old-user"})
    assert not missing["passed"]
    assert missing["missing_references"] == [("runtime_event", "tool-1")]


def test_activity_benchmark_resolves_new_policy_id_before_scoring_same_turn():
    from lisa.models.policies import PolicyRecord

    services = SimpleNamespace(
        policy_service=SimpleNamespace(
            list_policy_records=lambda: [PolicyRecord("P1.3", "workspace", None, "Refund", "Review", False)]
        ),
        activity_store=SimpleNamespace(
            list_activity_feed=lambda **_: [], list_activity_followups=lambda _: [],
        ),
    )
    substitutions = {}
    _update_substitutions(services, substitutions)
    scored = _score_turn(
        ActivityTurn(chat="main", text="Create policy", reply_must_include=["$policy_id"]),
        {"activities": [], "messages": [{"role": "assistant", "text": "Policy ID: P1.3"}]},
        substitutions, 0,
    )
    assert substitutions == {"policy_id": "P1.3"}
    assert all(check["passed"] for check in scored["communication_checks"])

def test_activity_benchmark_rate_preserves_empty_denominator():
    assert _rate([]) is None
    assert _rate([{"passed": True}, {"passed": False}]) == 0.5


def test_activity_workflow_uses_activity_tools_without_database_or_session_search():
    prompt = " ".join(LISA_SYSTEM_PROMPT.casefold().split())

    assert "activity management tasks, use the activity tools" in prompt
    assert "do not query lisa_db or search sessions" in prompt
    assert "continuation in a later turn" in prompt
    assert "reuse that activity id" in prompt
    assert "do not call create_activity or list_recent_activities" in prompt
    assert "route by the user's requested outcome" in prompt
    assert "call get_activity_thread next" in prompt
    assert "entire owner message" in prompt or "only that standalone owner command" in prompt
    assert "use list_policies and read_policy" in prompt
    assert "never call lisa_db to inspect or change policy rows" in prompt
    assert "omit policy_id" in prompt
    assert "assigns the next readable id" in prompt
    assert "never guess or reuse a policy id" in prompt
    database_schema = " ".join(LISA_DATABASE_SCHEMA.casefold().split())
    assert "retrieve lisa's recorded evaluation history" in database_schema
    assert "do not query lisa_db to find, create, update, or inspect activities" in database_schema
    assert "delete_policy" in database_schema

    policy_skill = " ".join((
        Path(__file__).resolve().parents[1] / "skills/learn-agent-policy/SKILL.md"
    ).read_text().split())
    assert "write_policy" in policy_skill
    assert "delete_policy" in policy_skill
    assert "Delete policy: <policy_id>" in policy_skill
    assert "refuses deletion" in policy_skill
    assert "Approve policy: <policy_id>" in policy_skill
    assert "update the existing row in place" in policy_skill
    assert "Keep the same policy ID" in policy_skill
    assert "updates that same row" in policy_skill
    assert "P1.1` and `P1.1.1` are different" in policy_skill
    assert "omit `policy_id`" in policy_skill
    assert "allocates and returns the next" in policy_skill
    assert "do not call `write_policy`, `approve_policy`, or `delete_policy`" in policy_skill
    assert "Do not use `lisa_db` for normal policy work." in policy_skill


def test_activity_labels_cover_timeline_evidence_and_exact_followup_contract():
    labels = ActivityLabels.model_validate(
        {
            "activity_count": 1,
            "activities": [
                {
                    "title_contains": "Aspen",
                    "status": "succeeded",
                    "timeline_text_contains": ["same idempotency key", "retry worker"],
                }
            ],
            "followups": [
                {
                    "activity_title_contains": "Aspen",
                    "status": "pending",
                    "count": 1,
                    "trigger_event_type": "deployment_completed",
                    "prompt_contains": ["review", "health"],
                }
            ],
        }
    )

    activity = {"id": "a1", "title": "Aspen duplicate settlement", "status": "succeeded"}
    services = SimpleNamespace(
        activity_store=SimpleNamespace(
            list_activity_feed=lambda **_: [activity],
            get_activity_thread=lambda _: {
                "actions": [
                    {
                        "event_type": "completed",
                        "summary": "Same idempotency key; retry worker ignored it",
                    }
                ]
            },
            list_activity_followups=lambda _: [
                {
                    "status": "pending",
                    "trigger_event_type": "deployment_completed",
                    "prompt": "Review deployment health once",
                }
            ],
        ),
        policy_service=SimpleNamespace(read_policy_records=list, list_policy_records=list),
    )

    checks = _score_state(services, labels.model_dump(exclude_unset=True))

    assert all(check["passed"] for check in checks)
    services.activity_store.list_activity_followups = lambda _: [
        {
            "status": "pending",
            "trigger_event_type": "deployment_completed",
            "prompt": "Review deployment health once",
        },
        {
            "status": "pending",
            "trigger_event_type": "deployment_completed",
            "prompt": "Review deployment health twice",
        },
    ]
    assert not next(
        check
        for check in _score_state(services, labels.model_dump())
        if check["check"].startswith("followup:")
    )["passed"]
