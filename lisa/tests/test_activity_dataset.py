"""Integrity and deterministic scoring contracts for the agent benchmark dataset."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmark.activity_runner import (
    _interpolate,
    _rate,
    _score_activity_integrity,
    _score_state,
    _score_trajectory_links,
    _score_turn,
    _select_cases,
    _update_substitutions,
)
from datasets.activity_dataset import ActivityLabels, ActivityTurn, load_activity_dataset
from lisa.backends.openhands import LISA_SYSTEM_PROMPT
from lisa.repositories.lisa_database import POLICY_COLUMNS
from lisa.views.database import LISA_DATABASE_SCHEMA

DATASET = Path(__file__).resolve().parents[1] / "datasets/activity_management"


def _feed_row(activity_id, title, status="pending"):
    return {
        "id": activity_id, "kind": "task", "title": title, "status": status,
        "created_at": "2026-10-21T00:00:00Z", "updated_at": "2026-10-21T00:00:00Z",
        "finished_at": "2026-10-21T00:01:00Z" if status in {"success", "error", "stopped"} else None,
        "details_json": {}, "next_action": "Continue" if status == "blocked" else None,
    }


def test_activity_benchmark_release_has_frozen_cases_and_both_splits():
    manifest, cases, digest = load_activity_dataset(DATASET / "manifests/benchmark.json")

    assert manifest.case_count == len(cases) == 32
    tracking_cases = [
        case for case in cases
        if case.split == "benchmark" and case.case_id.startswith("activity_tracking_")
    ]
    assert len(tracking_cases) == 10
    assert all(case.expected.activities for case in tracking_cases)
    assert any(len(case.turns) > 1 for case in tracking_cases)
    assert any(turn.chat == "slack" for case in tracking_cases for turn in case.turns)
    assert any(case.expected.activity_count == 2 for case in tracking_cases)
    assert any(
        activity.status == "success"
        for case in tracking_cases for activity in case.expected.activities
    )
    by_id = {case.case_id: case for case in tracking_cases}
    assert by_id["activity_tracking_database_migration"].expected.activities[0].status == "blocked"
    assert by_id["activity_tracking_database_migration"].expected.activities[0].next_action_contains
    assert by_id["activity_tracking_webhook_retry"].expected.activities[0].status == "blocked"
    independent = by_id["activity_tracking_billing_reconciliation"]
    assert independent.expected.activity_count == 2
    assert all(item.status == "blocked" and item.next_action_contains for item in independent.expected.activities)
    assert digest
    assert {case.split for case in cases} == {"development", "benchmark"}
    assert {case.category for case in cases} >= {
        "create",
        "update",
        "continuity",
        "policy",
        "evaluation",
        "request_resolution",
    }
    assert len({case.case_id for case in cases}) == len(cases)
    assert any(case.category == "database" for case in cases)


def test_activity_benchmark_can_select_new_heldout_tracking_cases_without_changing_manifest():
    manifest, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    selected = _select_cases(cases, "benchmark", "activity_tracking_")

    assert manifest.case_count == len(cases)
    assert len(selected) == 10
    assert all(case.split == "benchmark" for case in selected)
    history_cases = [case for case in cases if case.category == "evaluation"]
    assert len(history_cases) == 3
    assert all(case.historical_evaluations for case in history_cases)
    for case_id in ("find_historical_judge_verdict", "report_historical_triage_and_judge"):
        case = next(case for case in history_cases if case.case_id == case_id)
        assert case.expected.activity_count == 2
        assert case.expected.activities[0].status == "success"
        assert case.turns[0].tool_order == ["resolve_chat_activity", "lisa_db"]


def test_historical_evaluation_fixture_seeds_and_acknowledges_its_score_outbox():
    fixture = (DATASET.parent.parent / "benchmark" / "sql" / "complete_historical_evaluation.sql").read_text()

    assert "INSERT INTO lisa.score_outbox" in fixture
    assert "%(score_payload)s::jsonb" in fixture
    assert "%(payload_hash)s" in fixture
    assert "acknowledged_at" in fixture
    assert "'acknowledged', 1" in fixture
    assert "SELECT seeded_outbox.id" in fixture


def test_activity_request_resolution_benchmark_covers_skip_split_failure_and_multitask():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    by_id = {case.case_id: case for case in cases}

    assert by_id["greeting_is_explicitly_skipped"].turns[0].track_activity is False
    explicit_skip = by_id["answer_simple_question_creates_request_activity"]
    assert explicit_skip.turns[0].track_activity is False
    assert explicit_skip.expected.activity_count == 0
    independent = by_id["independent_requests_get_separate_activities"]
    assert independent.turns[0].tool_call_counts["resolve_chat_activity"] == 2
    assert independent.turns[0].tool_order == [
        "resolve_chat_activity", "complete_activity",
        "resolve_chat_activity", "complete_activity",
    ]
    assert "lisa_db" not in independent.turns[0].require_tools
    assert independent.expected.activity_count == 2
    assert by_id["independent_requests_get_separate_activities"].turns[0].tool_call_counts[
        "resolve_chat_activity"
    ] == 2
    assert by_id["clarify_ambiguous_activity_before_update"].expected.activity_count == 3
    clarification = by_id["clarify_ambiguous_activity_before_update"].turns[2]
    assert "set_activity_progress" in clarification.require_tools
    assert {"update_activity", "complete_activity"} <= set(clarification.forbid_tools)
    assert by_id["closed_activity_continuation_starts_new_work"].expected.activity_count == 2
    failed = by_id["missing_session_failure_is_recorded"]
    assert failed.expected.activities[0].status == "error"
    assert "session-does-not-exist-activity-check" in failed.expected.activities[0].timeline_text_contains
    assert len(failed.turns) == 2
    assert failed.turns[0].tool_call_counts == {"resolve_chat_activity": 1, "set_activity_progress": 1}
    assert failed.turns[1].tool_call_counts == {"resolve_chat_activity": 1, "evaluate_session": 1}
    assert failed.turns[1].track_activity


def test_development_activity_cases_exercise_user_choice_and_pending_clarification():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    by_id = {case.case_id: case for case in cases}

    explicit_skip = by_id["owner_explicitly_skips_brief_question"]
    assert explicit_skip.split == "development"
    assert explicit_skip.turns[0].track_activity is False
    assert explicit_skip.turns[0].require_tools == ["skip_chat_activity_for_turn"]
    assert explicit_skip.expected.activity_count == 0

    clarification = by_id["ambiguous_update_waits_for_owner_choice"]
    assert clarification.split == "development"
    assert clarification.expected.activities[2].status == "blocked"
    assert "set_activity_progress" in clarification.turns[2].require_tools
    assert {"update_activity", "complete_activity"} <= set(
        clarification.turns[2].forbid_tools
    )
    pending_clarification = clarification.expected.activities[2]
    assert "clarify checkout retry failure" in pending_clarification.title_contains.casefold()


def test_pending_policy_activities_are_waiting_in_the_public_feed():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    by_id = {case.case_id: case for case in cases}
    for case_id in (
        "create_unapproved_policy",
        "policy_approval_with_extra_text",
        "wrong_policy_id_stays_unapproved",
    ):
        labels = by_id[case_id].expected.activities
        assert labels and labels[0].status == "blocked"
    composite = by_id["create_approve_and_update_policy_in_place"].expected.activities
    assert [item.status for item in composite] == ["success", "blocked"]


def test_each_tracked_chat_turn_orders_resolution_before_business_work():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    for case in cases:
        for turn in case.turns:
            if turn.track_activity is False:
                continue
            assert "resolve_chat_activity" in turn.require_tools, case.case_id
            assert "resolve_chat_activity" in turn.tool_order, case.case_id
            if "find_activity" in turn.require_tools:
                assert turn.tool_order.index("find_activity") < turn.tool_order.index(
                    "resolve_chat_activity"
                ), case.case_id
            if "get_activity_thread" in turn.require_tools:
                assert turn.tool_order.index("get_activity_thread") < turn.tool_order.index(
                    "resolve_chat_activity"
                ), case.case_id


def test_activity_benchmark_covers_policy_creation_approval_and_in_place_update():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    case = next(case for case in cases if case.case_id == "create_approve_and_update_policy_in_place")

    assert case.category == "policy"
    assert case.split == "benchmark"
    assert [turn.require_tools for turn in case.turns] == [
        ["write_policy", "resolve_chat_activity"],
        ["approve_policy", "resolve_chat_activity", "find_activity", "get_activity_thread"],
        ["write_policy", "resolve_chat_activity", "find_activity"],
    ]
    assert "Approve policy: {policy_id}" == case.turns[1].text
    assert "{policy_id}" in case.turns[2].text
    assert case.turns[2].tool_arguments_must_include == ["P1.1", "Refund Approval Guard"]
    assert case.turns[2].reply_must_include == ["P1.1", "approved: no"]
    assert case.expected.policy_count == 1
    assert case.expected.pending_policy_count == 1
    assert case.expected.activity_count == 2


def test_activity_benchmark_deletes_existing_policy_and_checks_database_absence():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    case = next(case for case in cases if case.case_id == "delete_existing_policy")

    assert case.category == "policy"
    assert case.split == "benchmark"
    assert [turn.require_tools for turn in case.turns] == [
        ["write_policy", "resolve_chat_activity"],
        ["delete_policy", "resolve_chat_activity"],
    ]
    assert case.turns[1].text == "Lisa, please get rid of policy {policy_id}; we no longer need that rule."
    assert case.turns[1].channel != case.turns[0].channel
    assert case.expected.activity_count is None
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
        chat="slack", text="Delete policy {policy_id}",
        require_tools=["delete_policy", "resolve_chat_activity"],
        tool_arguments_must_include=["$policy_id"],
    )
    result = {
        "activities": [
            {"kind": "tool", "tool_name": "delete_policy", "arguments": {"policy_id": "P1.1"}},
            {"kind": "tool", "tool_name": "resolve_chat_activity", "arguments": {"activity_id": "activity-1"}},
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


def test_activity_benchmark_fails_unexpected_tool_errors():
    turn = ActivityTurn(
        chat="main",
        text="Inspect the policy catalog",
        require_tools=["lisa_db"],
    )
    result = {
        "activities": [
            {"kind": "tool", "tool_name": "lisa_db", "tool_call_id": "db-1"},
            {
                "kind": "evidence",
                "tool_name": "lisa_db",
                "tool_call_id": "db-1",
                "is_error": True,
                "detail": "Column archived_at is not available from an approved view",
            },
        ],
        "messages": [{"role": "assistant", "text": "I could not read the policies."}],
    }

    scored = _score_turn(turn, result, {}, 1)

    errors = next(check for check in scored["tool_checks"] if check["check"] == "tool_errors")
    assert not errors["passed"]
    assert errors["actual"] == ["lisa_db"]


def test_activity_benchmark_allows_labeled_expected_tool_errors():
    turn = ActivityTurn(
        chat="main",
        text="Evaluate a missing session",
        expected_tool_errors=["evaluate_session"],
    )
    result = {
        "activities": [
            {
                "kind": "evidence",
                "tool_name": "evaluate_session",
                "is_error": True,
                "detail": "Session not found",
            },
        ],
        "messages": [],
    }

    scored = _score_turn(turn, result, {}, 1)

    assert next(check for check in scored["tool_checks"] if check["check"] == "tool_errors")["passed"]


def test_activity_benchmark_includes_policy_catalog_schema_case():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    case = next(case for case in cases if case.case_id == "inspect_policy_catalog")

    assert case.split == "benchmark"
    assert case.turns[0].require_tools == ["lisa_db", "resolve_chat_activity"]
    assert "archived_at" in case.turns[0].tool_arguments_must_not_include
    assert {
        "id", "scope", "target_agent_name", "name", "criteria", "approved",
        "created_at", "updated_at",
    } <= set(case.turns[0].tool_arguments_must_include)


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
        tool_call_counts={"resolve_chat_activity": 0, "find_activity": 1},
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
    result["activities"].append({"kind": "tool", "tool_name": "resolve_chat_activity"})
    scored = _score_turn(turn, result, {}, 0)
    assert not next(
        check
        for check in scored["tool_checks"]
        if check["check"] == "tool_call_count:resolve_chat_activity"
    )["passed"]


def test_activity_benchmark_requires_selected_chat_and_runtime_events_to_be_linked():
    class ActivityStore:
        def __init__(self, references):
            self.references = references

        def get_activity_thread(self, activity_id):
            return {
                "references": self.references[activity_id],
                "actions": [
                    {"seq": index, "event_type": "chat.user", "title": "Recorded item", "created_at": "now",
                     "reference_type": item["reference_type"], "reference_id": item["reference_id"]}
                    for index, item in enumerate(self.references[activity_id], 1)
                ],
            }

    result = {
        "messages": [
            {"id": "old-user", "role": "user"},
            {"id": "new-user", "role": "user"},
            {"id": "new-assistant", "role": "assistant"},
        ],
        "activities": [
            {"id": "resolve-1", "kind": "tool", "tool_name": "resolve_chat_activity", "sequence": 1},
            {"id": "resolved-1", "kind": "evidence", "tool_name": "resolve_chat_activity", "detail": '{"disposition":"create","activity_id":"activity-1"}', "sequence": 2},
            {"id": "tool-1", "kind": "tool", "tool_name": "write_policy", "sequence": 3},
        ],
        "chat_activity_ids": ["activity-1"],
    }
    linked = {"activity-1": [
        {"reference_type": "message", "reference_id": "new-user"},
        {"reference_type": "message", "reference_id": "new-assistant"},
        {"reference_type": "runtime_event", "reference_id": "resolve-1"},
        {"reference_type": "runtime_event", "reference_id": "resolved-1"},
        {"reference_type": "runtime_event", "reference_id": "tool-1"},
    ]}
    assert _score_trajectory_links(ActivityStore(linked), result, {"old-user"})["passed"]
    missing = _score_trajectory_links(ActivityStore({"activity-1": linked["activity-1"][:2]}), result, {"old-user"})
    assert not missing["passed"]
    assert missing["missing_references"]

    cross_scope = {**result, "activities": [
        *result["activities"],
        {"id": "other-tool", "kind": "tool", "tool_name": "complete_activity",
         "arguments": {"activity_id": "activity-2"}, "sequence": 4},
    ]}
    overlinked = {"activity-1": [
        *linked["activity-1"],
        {"reference_type": "runtime_event", "reference_id": "other-tool"},
    ]}
    leaked = _score_trajectory_links(ActivityStore(overlinked), cross_scope, {"old-user"})
    assert not leaked["passed"]
    assert leaked["unexpected_references"]


def test_activity_benchmark_rejects_malformed_feed_rows():
    check = _score_activity_integrity([
        {
            "id": "not-a-uuid",
            "kind": "chat_request",
            "title": "",
            "status": "pending",
            "created_at": "2026-10-21T00:00:00Z",
            "updated_at": "2026-10-21T00:00:00Z",
            "details_json": [],
        },
    ])

    assert not check["passed"]
    assert {issue["field"] for issue in check["issues"]} >= {
        "id", "title", "details_json",
    }


def test_activity_benchmark_resolves_new_policy_id_before_scoring_same_turn():
    from lisa.models.policies import PolicyRecord

    services = SimpleNamespace(
        policy_service=SimpleNamespace(
            list_policy_records=lambda: [PolicyRecord("P1.3", "workspace", None, "Refund", "Review", False)]
        ),
        activity_store=SimpleNamespace(
            list_activity_feed=lambda **_: [],
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
    assert "call resolve_chat_activity immediately before any other tool call" in prompt
    assert "route by the user's requested outcome" in prompt
    assert "call get_activity_thread next" in prompt
    assert "if the match is closed, inspect its thread before resolving" in prompt
    assert "for a chat session evaluation, resolve the request and call evaluate_session" in prompt
    assert "the tool reuses the active resolved activity" in prompt
    assert "resolve each independent request immediately before doing its task-specific work" in prompt
    assert "resolve every request before the final reply" in prompt
    assert "never create a second activity just because the next action is approval" in prompt
    assert "keep a newly created or updated policy's activity in `waiting`" in prompt
    assert "when an authorized owner asks to approve a policy, call approve_policy with the policy id they named" in prompt
    assert "use list_policies and read_policy" in prompt
    assert "for an explicit request to inspect policy catalog rows or run sql diagnostics, lisa_db may read lisa.policies" in prompt
    assert "it has no archived_at column, and deletion removes the row" in prompt
    assert ", ".join(POLICY_COLUMNS) in prompt
    assert "never use lisa_db to create, update, approve, or delete policies" in prompt
    assert "never call lisa_db to inspect or change policy rows" not in prompt
    assert "same work item, including when it arrives in another chat" in prompt
    assert "before calling approve_policy, verify the exact policy id and its current approval state with list_policies or read_policy" in prompt
    assert "if `activity_finalized`" in prompt
    assert "the service could not finalize the activity" in prompt
    assert "do not call `get_session`, `complete_activity`, or `set_activity_progress` again" in prompt
    assert "omit policy_id" in prompt
    assert "assigns the next readable id" in prompt
    assert "never guess or reuse a policy id" in prompt
    assert "owner asks to delete a policy, verify the exact current id with list_policies or read_policy and call delete_policy" in prompt
    database_schema = " ".join(LISA_DATABASE_SCHEMA.casefold().split())
    assert "retrieve lisa's recorded evaluation history" in database_schema
    assert "do not query lisa_db to find, create, update, or inspect activities" in database_schema
    assert "delete_policy" in database_schema
    assert "there is no archived_at column" in database_schema
    assert ", ".join(POLICY_COLUMNS) in database_schema
    assert "truncated" in database_schema
    assert "wildcard projections are rejected" in database_schema

    policy_skill = " ".join((
        Path(__file__).resolve().parents[1] / "skills/learn-agent-policy/SKILL.md"
    ).read_text().split())
    qa_skill = " ".join((
        Path(__file__).resolve().parents[1] / "skills/own-agent-qa/SKILL.md"
    ).read_text().casefold().split())
    evaluation_skill = " ".join((
        Path(__file__).resolve().parents[2]
        / ".agents/skills/run-lisa-policy-evaluation/SKILL.md"
    ).read_text().casefold().split())
    assert "for a chat evaluation, resolve the request first" in qa_skill
    assert "evaluate_session` reuses that active activity" in qa_skill
    assert "when the result includes `activity_finalized: false`" in qa_skill
    assert "write_policy" in policy_skill
    assert "delete_policy" in policy_skill
    assert "When an authorized owner asks to delete a policy" in policy_skill
    assert "Historical evaluation rows retain the policy ID" in policy_skill
    assert "call `delete_policy` for that ID" in policy_skill
    assert "owners can request policy deletion in natural language" in evaluation_skill
    assert "historical evaluation rows retain the policy id, name, and criteria hash" in evaluation_skill
    assert "verify the exact policy ID and its current approval state with `list_policies` or `read_policy`" in policy_skill
    assert "call `approve_policy` with the policy ID they named" in policy_skill
    assert "update the existing row in place" in policy_skill
    assert "Keep the same policy ID" in policy_skill
    assert "updates that same row" in policy_skill
    assert "P1.1` and `P1.1.1` are different" in policy_skill
    assert "omit `policy_id`" in policy_skill
    assert "allocates and returns the next" in policy_skill
    assert "do not call `write_policy`, `approve_policy`, or `delete_policy`" in policy_skill
    assert "Do not use `lisa_db` for normal policy work." in policy_skill
    assert "it has no `archived_at`" in policy_skill
    assert all(column in policy_skill for column in POLICY_COLUMNS)
    assert 'set_activity_progress(status="waiting")' in policy_skill


def test_clarification_activity_can_be_set_waiting_but_not_changed_again_before_answer():
    project_root = Path(__file__).resolve().parents[1]
    prompt = " ".join((project_root / "src/lisa/backends/openhands.py").read_text().split())
    activity_skill = " ".join((project_root / "skills/manage-lisa-activities/SKILL.md").read_text().split())

    assert "set it to waiting, ask which candidate the user means, then make no further activity changes until the user answers" in prompt
    assert "set it to `waiting`, list the candidates, and ask which one the user means. After that, make no further activity changes until the user answers." in activity_skill


def test_activity_labels_cover_timeline_evidence():
    labels = ActivityLabels.model_validate(
        {
            "activity_count": 1,
            "activities": [
                {
                    "title_contains": "Aspen",
                    "status": "success",
                    "timeline_text_contains": ["same idempotency key", "retry worker"],
                }
            ],
        }
    )

    activity = _feed_row("00000000-0000-4000-8000-000000000001", "Aspen duplicate settlement", "success")
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
        ),
        policy_service=SimpleNamespace(read_policy_records=list, list_policy_records=list),
    )

    checks = _score_state(services, labels.model_dump(exclude_unset=True))

    assert all(check["passed"] for check in checks)


def test_activity_count_expectation_is_optional_for_policy_state_cases():
    labels = ActivityLabels.model_validate({"policy_count": 0})
    services = SimpleNamespace(
        activity_store=SimpleNamespace(list_activity_feed=lambda **_: []),
        policy_service=SimpleNamespace(list_policy_records=list),
    )

    checks = _score_state(services, labels.model_dump(exclude_unset=True))

    assert [check["check"] for check in checks] == ["activity_feed_well_formed", "policy_count"]


def test_activity_label_matches_any_duplicate_title_that_satisfies_lifecycle():
    labels = ActivityLabels.model_validate(
        {
            "activity_count": 2,
            "activities": [{"title_contains": "Cedar 901", "status": "success"}],
        }
    )
    feed = [
        _feed_row("00000000-0000-4000-8000-000000000002", "Cedar 901 completion check"),
        _feed_row("00000000-0000-4000-8000-000000000003", "Cedar 901 completion check", "success"),
    ]
    services = SimpleNamespace(
        activity_store=SimpleNamespace(list_activity_feed=lambda **_: feed),
        policy_service=SimpleNamespace(read_policy_records=list, list_policy_records=list),
    )

    checks = _score_state(services, labels.model_dump(exclude_unset=True))

    assert all(check["passed"] for check in checks)
