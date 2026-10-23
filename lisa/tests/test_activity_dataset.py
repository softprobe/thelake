"""Integrity and deterministic scoring contracts for the agent benchmark dataset."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmark.activity_runner import (
    _interpolate,
    _memory_acceptance,
    _memory_case_scores,
    _benchmark_status,
    _restore_complete_activity_memory_results,
    _rate,
    _score_activity_integrity,
    _score_resolved_activity_scope,
    _score_state,
    _score_trajectory_links,
    _score_turn,
    _select_cases,
    _checks_pass_for_case,
    _score_activity_memory_turn,
    _turn_requires_verbatim_answer,
    _update_substitutions,
)
from datasets.activity_dataset import ActivityLabels, ActivityTurn, load_activity_dataset
from lisa.backends.openhands import LISA_SYSTEM_PROMPT
from lisa.repositories.lisa_database import POLICY_COLUMNS
from lisa.views.database import LISA_DATABASE_SCHEMA

DATASET = Path(__file__).resolve().parents[1] / "datasets/activity_management"
MEMORY_DATASET = Path(__file__).resolve().parents[1] / "datasets/activity_memory"


def test_live_benchmark_status_fails_for_labeled_case_failures():
    assert _benchmark_status([{"passed": False}], []) == "failed"
    assert _benchmark_status([{"passed": True}], []) == "completed"
    assert _benchmark_status([{"passed": True}], [{"case_id": "case"}]) == "failed"


def test_benchmark_interpolation_preserves_literal_braces_in_user_text():
    assert _interpolate(
        'The recorded label is {task name}; policy is {policy_id} ($policy_id).',
        {"policy_id": "P1.2"},
    ) == "The recorded label is {task name}; policy is P1.2 (P1.2)."


def test_memory_case_scores_report_answer_selection_reference_integrity_and_abstention():
    result = {
        "memory_ability": "memory_abstention",
        "passed": True,
        "tool_checks": [
            {"check": "resolved_activity_scope", "passed": True},
            {"check": "activity_memory_selection", "passed": True},
            {"check": "activity_memory_evidence_refs", "passed": True},
        ],
        "communication_checks": [{"check": "expected_abstention", "passed": True}],
    }
    assert _memory_case_scores(result) == {
        "answer_success": 1.0,
        "activity_selection": 1.0,
        "evidence_reference_integrity": 1.0,
        "abstention": 1.0,
        "verbatim_repeat": None,
        "tool_sequence_adherence": None,
        "tool_error_free": None,
        "core_correct": True,
    }


@pytest.mark.parametrize(
    "reply",
    [
        "This activity did not record an approval ID.",
        "No launch date was recorded for this activity.",
    ],
)
def test_abstention_rubric_accepts_clear_missing_record_wording(reply):
    turn = ActivityTurn(chat="main", text="What did this activity record?", expects_abstention=True)
    scored = _score_turn(
        turn,
        {"activities": [], "messages": [{"role": "assistant", "text": reply}]},
        {},
        1,
    )

    assert next(
        item for item in scored["communication_checks"]
        if item["check"] == "expected_abstention"
    )["passed"]


@pytest.mark.parametrize(
    "reply",
    [
        "The information provided is not enough to determine the answer.",
        "That value was never recorded for this activity.",
        "I will not guess. Which activity do you mean?",
    ],
)
def test_abstention_rubric_accepts_insufficient_source_and_clarification(reply):
    turn = ActivityTurn(chat="main", text="Answer from memory.", expects_abstention=True)
    scored = _score_turn(
        turn,
        {"activities": [], "messages": [{"role": "assistant", "text": reply}]},
        {},
        1,
    )

    assert next(
        item for item in scored["communication_checks"]
        if item["check"] == "expected_abstention"
    )["passed"]


def test_abstention_rubric_rejects_a_missing_record_claim_followed_by_a_leaked_answer():
    from benchmark.activity_runner import _reply_indicates_abstention

    assert not _reply_indicates_abstention(
        "No launch date was recorded, but it was November 16.",
        forbidden_values=["November 16"],
    )


def test_recorded_answer_quote_rubric_requires_the_exact_source_text():
    quote = "The exact recorded answer, including punctuation."
    turn = ActivityTurn(
        chat="main",
        text="Repeat the recorded result briefly.",
        reply_must_quote=[quote],
    )

    exact = _score_turn(
        turn,
        {"activities": [], "messages": [{"role": "assistant", "text": f"It was: {quote}"}]},
        {},
        1,
    )
    paraphrase = _score_turn(
        turn,
        {"activities": [], "messages": [{"role": "assistant", "text": "The answer was essentially the same."}]},
        {},
        1,
    )

    assert next(item for item in exact["communication_checks"] if item["check"].startswith("reply_quotes_exactly:"))["passed"]
    assert not next(item for item in paraphrase["communication_checks"] if item["check"].startswith("reply_quotes_exactly:"))["passed"]


def test_verbatim_requirement_needs_an_explicit_exactness_request():
    assert not _turn_requires_verbatim_answer("Repeat the recorded result briefly.")
    assert _turn_requires_verbatim_answer("Quote the recorded result verbatim.")


def test_nonrequired_exact_quote_is_diagnostic_not_a_case_gate():
    check = {
        "check": "reply_quotes_exactly:the recorded answer",
        "passed": False,
        "required_for_answer_success": False,
    }

    assert _checks_pass_for_case([check])
    assert not _checks_pass_for_case([{**check, "required_for_answer_success": True}])


def test_brief_recall_keeps_verbatim_fidelity_as_a_separate_metric():
    result = {
        "memory_ability": "memory_information_extraction",
        "tool_checks": [
            {"check": "activity_memory_selection", "passed": True},
            {"check": "activity_memory_evidence_refs", "passed": True},
        ],
        "communication_checks": [
            {"check": "reply_contains:the recorded answer", "passed": True},
            {
                "check": "reply_quotes_exactly:the recorded answer",
                "passed": False,
                "required_for_answer_success": False,
            },
        ],
    }

    scores = _memory_case_scores(result)

    assert scores["answer_success"] == 1.0
    assert scores["verbatim_repeat"] == 0.0
    assert scores["core_correct"] is True


def test_memory_answer_success_includes_exact_repeat_requirements():
    result = {
        "memory_ability": "memory_information_extraction",
        "tool_checks": [],
        "communication_checks": [
            {
                "check": "reply_quotes_exactly:the answer",
                "passed": False,
                "required_for_answer_success": True,
            },
        ],
    }

    assert _memory_case_scores(result)["answer_success"] == 0.0


def test_memory_correctness_separates_task_outcomes_from_tool_procedure_diagnostics():
    cases = [
        {
            "case_id": str(index),
            "memory_ability": "memory_information_extraction",
            "passed": False,
            "tool_checks": [
                {"check": "activity_memory_selection", "passed": True},
                {"check": "activity_memory_evidence_refs", "passed": True},
                {"check": "resolved_activity_scope", "passed": True, "wrong_write_targets": []},
                {"check": "required_tool:find_activity", "passed": False},
                {"check": "tool_order:find_activity->get_activity_thread->resolve_chat_activity->load_activity_memory", "passed": False},
                {"check": "tool_errors", "passed": False, "actual": ["invoke_skill"]},
            ],
            "communication_checks": [
                {"check": "reply_contains:recorded fact", "passed": True},
            ],
        }
        for index in range(35)
    ]

    accepted = _memory_acceptance(cases)

    assert accepted["correct_cases"] == 35
    assert accepted["tool_sequence_failures"] == 70
    assert accepted["unexpected_tool_error_cases"] == 35
    assert accepted["accepted"] is True


def test_memory_correctness_requires_answer_selection_evidence_and_write_scope():
    result = {
        "memory_ability": "memory_information_extraction",
        "tool_checks": [
            {"check": "activity_memory_selection", "passed": True},
            {"check": "activity_memory_evidence_refs", "passed": True},
            {"check": "resolved_activity_scope", "passed": True, "wrong_write_targets": []},
        ],
        "communication_checks": [
            {"check": "reply_contains:recorded fact", "passed": True},
        ],
    }

    assert _memory_case_scores(result)["core_correct"] is True

    result["tool_checks"][0]["passed"] = False
    assert _memory_case_scores(result)["core_correct"] is False

    result["tool_checks"][0]["passed"] = True
    result["tool_checks"][1]["passed"] = False
    assert _memory_case_scores(result)["core_correct"] is False

    result["tool_checks"][1]["passed"] = True
    result["tool_checks"][2]["wrong_write_targets"] = ["other-activity"]
    assert _memory_case_scores(result)["core_correct"] is False


def test_verbatim_quote_is_core_only_when_prompt_asks_to_repeat_recorded_result():
    expected = "The recorded answer has the exact wording."
    exact_repeat = _score_turn(
        ActivityTurn(
            chat="main",
            text="Quote the recorded result verbatim.",
            reply_must_quote=[expected],
        ),
        {"activities": [], "messages": [{"role": "assistant", "text": "The recorded answer has the **exact** wording."}]},
        {},
        1,
    )
    factual_recall = _score_turn(
        ActivityTurn(
            chat="main",
            text="What wording did I use?",
            reply_must_include=["exact wording"],
            reply_must_quote=[expected],
        ),
        {"activities": [], "messages": [{"role": "assistant", "text": "The exact wording."}]},
        {},
        1,
    )

    repeat_quote = next(
        item for item in exact_repeat["communication_checks"]
        if item["check"].startswith("reply_quotes_exactly:")
    )
    recall_quote = next(
        item for item in factual_recall["communication_checks"]
        if item["check"].startswith("reply_quotes_exactly:")
    )

    assert repeat_quote["passed"] is True
    assert repeat_quote["required_for_answer_success"] is True
    assert recall_quote["passed"] is False
    assert recall_quote["required_for_answer_success"] is False


def test_fact_anchor_rubric_still_accepts_equivalent_wording():
    turn = ActivityTurn(
        chat="main",
        text="What is recorded?",
        reply_must_include=["keep the default", "Assigned to"],
    )
    scored = _score_turn(
        turn,
        {
            "activities": [],
            "messages": [{
                "role": "assistant",
                "text": "Keep the default search. Assigned to should not be selected.",
            }],
        },
        {},
        1,
    )

    assert all(item["passed"] for item in scored["communication_checks"])


def test_either_or_memory_rubric_accepts_one_complete_answer_option():
    turn = ActivityTurn(
        chat="main",
        text="Give the most likely explanation or one action, not both.",
        reply_must_match_exactly_one_of=[
            {
                "must_include": ["crash is expected"],
                "must_not_include": ["not true that the crash is expected"],
            },
            {
                "must_include": ["keep the default", "for text"],
                "must_not_include": ["do not keep the default", "don't keep the default"],
            },
            {
                "must_include": ["directly open", "user's record"],
                "must_not_include": ["do not directly open", "don't directly open"],
            },
        ],
    )

    for reply in (
        "The crash is expected.",
        'Keep the default "for text" search.',
        "Directly open the user's record.",
    ):
        scored = _score_turn(
            turn,
            {"activities": [], "messages": [{"role": "assistant", "text": reply}]},
            {},
            1,
        )
        one_of_check = next(
            item for item in scored["communication_checks"]
            if item["check"] == "reply_matches_exactly_one_of"
        )
        assert one_of_check["passed"] is True

    for reply in (
        "It is not true that the crash is expected.",
        "Don't keep the default for text; use another field.",
        "Do not directly open the user's record.",
        "The crash is expected, so keep the default for text.",
        "The crash is expected, but don't keep the default for text.",
        "Try another option.",
    ):
        scored = _score_turn(
            turn,
            {"activities": [], "messages": [{"role": "assistant", "text": reply}]},
            {},
            1,
        )
        one_of_check = next(
            item for item in scored["communication_checks"]
            if item["check"] == "reply_matches_exactly_one_of"
        )
        assert one_of_check["passed"] is False
    memory_result = {
        "tool_checks": [
            {"check": "activity_memory_selection", "passed": True},
            {"check": "activity_memory_evidence_refs", "passed": True},
        ],
        "communication_checks": [one_of_check],
    }
    assert _memory_case_scores(memory_result)["core_correct"] is False
    memory_result["communication_checks"] = [
        {**one_of_check, "passed": True},
    ]
    assert _memory_case_scores(memory_result)["core_correct"] is True


def test_benchmark_restores_full_activity_memory_result_from_openhands_event(tmp_path):
    conversation_id = "11111111-1111-4111-8111-111111111111"
    observation_id = "22222222-2222-4222-8222-222222222222"
    complete_payload = {
        "activity_id": "activity-1",
        "status": "ready",
        "freshness_watermark": {"snapshot_event_seq": 12, "current_event_seq": 12},
        "snapshot": {"summary": "retained fact " + "x" * 6000},
        "source_event_refs": [
            {"sequence": 12, "reference_type": "activity_event", "reference_id": "12"}
        ],
    }
    conversation_dir = tmp_path
    event_dir = conversation_dir / conversation_id.replace("-", "") / "events"
    event_dir.mkdir(parents=True)
    event = {
        "id": observation_id,
        "tool_name": "load_activity_memory",
        "observation": {
            "content": [
                {"type": "text", "text": "[Tool 'load_activity_memory' executed.]"},
                {"type": "text", "text": json.dumps(complete_payload)},
            ]
        },
    }
    (event_dir / f"event-00007-{observation_id}.json").write_text(json.dumps(event))
    result = {
        "activities": [
            {
                "id": "33333333-3333-4333-8333-333333333333",
                "kind": "tool",
                "tool_name": "load_activity_memory",
                "tool_call_id": "call-1",
                "arguments": {"activity_id": "activity-1"},
            },
            {
                "id": observation_id,
                "kind": "evidence",
                "tool_name": "load_activity_memory",
                "tool_call_id": "call-1",
                "detail": '{"activity_id":"activity-1","status":"ready"',
            }
        ]
    }

    _restore_complete_activity_memory_results(result, conversation_dir, conversation_id)

    detail = result["activities"][1]["detail"]
    assert len(detail) > 4000
    assert "x" * 6000 in detail
    assert "source_event_refs" in detail

    event["observation"]["content"][1]["text"] = json.dumps(
        {**complete_payload, "snapshot": {"summary": "x" * 40_000}}
    )
    (event_dir / f"event-00007-{observation_id}.json").write_text(json.dumps(event))
    _restore_complete_activity_memory_results(result, conversation_dir, conversation_id)

    assert result["activities"][1]["memory_result_capture_truncated"] is True
    assert json.loads(result["activities"][1]["detail"]) == {"capture_truncated": True}
    checks = _score_activity_memory_turn(
        ActivityTurn(
            chat="main",
            text="Continue the recorded activity.",
            memory_activity_title_contains=["activity-1"],
        ),
        result,
        activity_store=SimpleNamespace(list_activity_feed=lambda **_: [{"id": "activity-1"}]),
    )
    evidence_check = next(
        check for check in checks if check["check"] == "activity_memory_evidence_refs"
    )
    assert evidence_check["passed"] is False
    assert evidence_check["failures"][0]["failure"] == (
        "tool result exceeds benchmark evidence capture limit"
    )


def test_benchmark_declines_oversized_openhands_event_file(tmp_path):
    conversation_id = "11111111-1111-4111-8111-111111111111"
    observation_id = "22222222-2222-4222-8222-222222222222"
    conversation_dir = tmp_path
    event_dir = conversation_dir / conversation_id.replace("-", "") / "events"
    event_dir.mkdir(parents=True)
    event = {
        "id": observation_id,
        "tool_name": "load_activity_memory",
        "observation": {
            "content": [
                {"type": "text", "text": json.dumps({"status": "ready"})}
            ]
        },
        # Event files have an envelope beyond the captured memory result. Keep
        # that envelope bounded too, even when the memory payload itself is small.
        "padding": "x" * (512 * 1024),
    }
    event_path = event_dir / f"event-00007-{observation_id}.json"
    event_path.write_text(json.dumps(event))
    original_detail = '{"status":"ready"'
    result = {
        "activities": [{
            "id": observation_id,
            "kind": "evidence",
            "tool_name": "load_activity_memory",
            "detail": original_detail,
        }]
    }

    _restore_complete_activity_memory_results(result, conversation_dir, conversation_id)

    assert result["activities"][0]["detail"] == '{"capture_truncated": true}'
    assert result["activities"][0]["memory_result_capture_truncated"] is True


def test_activity_memory_benchmark_checks_thread_reads_for_cross_task_text():
    turn = ActivityTurn(
        chat="main",
        text="Continue the invoice task.",
        memory_text_must_not_include=["CHARGE-594"],
    )
    result = {
        "activities": [
            {
                "id": "thread-call",
                "kind": "tool",
                "tool_name": "get_activity_thread",
                "tool_call_id": "thread-call-id",
                "arguments": {"activity_id": "invoice-593"},
            },
            {
                "id": "thread-result",
                "kind": "evidence",
                "tool_name": "get_activity_thread",
                "tool_call_id": "thread-call-id",
                "detail": '{"actions":[{"summary":"Invoice 594 used CHARGE-594"}]}',
            },
        ]
    }

    checks = _score_activity_memory_turn(turn, result, activity_store=None)

    leak_check = next(item for item in checks if item["check"] == "activity_thread_cross_task_isolation")
    assert leak_check["passed"] is False


def test_activity_memory_benchmark_checks_intentionally_contaminated_resolver_summary():
    turn = ActivityTurn(
        chat="main",
        text="Resolve Orchid with the cross-task reminder.",
        resolver_summary_must_include_by_title={
            "Orchid": ["MAPLE-SECRET-9"],
        },
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "resolve_chat_activity",
                "arguments": {
                    "disposition": "create",
                    "title": "Orchid task 9",
                    "summary": "Only the Orchid task details.",
                },
            },
        ],
        "messages": [{"role": "assistant", "text": "Done."}],
    }

    scored = _score_turn(turn, result, {}, index=0)
    check = next(
        item for item in scored["tool_checks"]
        if item["check"] == "resolver_summary_content:Orchid"
    )

    assert check["passed"] is False
    result["activities"][0]["arguments"]["summary"] = (
        "Orchid task with MAPLE-SECRET-9 as a related-work reminder."
    )
    rescored = _score_turn(turn, result, {}, index=0)
    matching_check = next(
        item for item in rescored["tool_checks"]
        if item["check"] == "resolver_summary_content:Orchid"
    )
    assert matching_check["passed"] is True


def test_activity_memory_benchmark_requires_task_scoped_resolver_summaries():
    turn = ActivityTurn(
        chat="main",
        text="Resolve Orchid without copying Maple details.",
        resolver_summary_must_not_include_by_title={
            "Orchid": ["MAPLE-SECRET-9"],
        },
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "resolve_chat_activity",
                "arguments": {
                    "disposition": "create",
                    "title": "Orchid task 9",
                    "summary": "Only the Orchid task details.",
                },
            },
        ],
        "messages": [{"role": "assistant", "text": "Done."}],
    }

    scored = _score_turn(turn, result, {}, index=0)
    check = next(
        item for item in scored["tool_checks"]
        if item["check"] == "resolver_summary_excludes:Orchid"
    )
    assert check["passed"] is True

    result["activities"][0]["arguments"]["summary"] = (
        "Orchid task with MAPLE-SECRET-9 as a related-work reminder."
    )
    rescored = _score_turn(turn, result, {}, index=0)
    contaminated_check = next(
        item for item in rescored["tool_checks"]
        if item["check"] == "resolver_summary_excludes:Orchid"
    )
    assert contaminated_check["passed"] is False


def test_activity_memory_benchmark_checks_intentionally_contaminated_resolver_title():
    turn = ActivityTurn(
        chat="main",
        text="Resolve Orchid with the cross-task title marker.",
        resolver_title_must_include_by_title={
            "Orchid": ["MAPLE-TITLE-SECRET-9"],
        },
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "resolve_chat_activity",
                "arguments": {
                    "disposition": "create",
                    "title": "Orchid task 9",
                },
            },
        ],
        "messages": [{"role": "assistant", "text": "Done."}],
    }

    scored = _score_turn(turn, result, {}, index=0)
    check = next(
        item for item in scored["tool_checks"]
        if item["check"] == "resolver_title_content:Orchid"
    )
    assert check["passed"] is False

    result["activities"][0]["arguments"]["title"] = (
        "Orchid task 9, Maple ID MAPLE-TITLE-SECRET-9"
    )
    rescored = _score_turn(turn, result, {}, index=0)
    matching_check = next(
        item for item in rescored["tool_checks"]
        if item["check"] == "resolver_title_content:Orchid"
    )
    assert matching_check["passed"] is True


def test_memory_heldout_gate_requires_32_cases_clean_evidence_and_no_cross_task_writes():
    cases = [
        {
            "case_id": str(index),
            "memory_ability": "memory_information_extraction",
            "passed": index < 33,
            "tool_checks": [
                {"check": "activity_memory_selection", "passed": True},
                {"check": "activity_memory_evidence_refs", "passed": True},
                {"check": "resolved_activity_scope", "passed": True},
            ],
            "communication_checks": [
                {"check": "reply_contains:recorded fact", "passed": index < 33},
            ],
        }
        for index in range(35)
    ]
    accepted = _memory_acceptance(cases)
    assert accepted["correct_cases"] == 33
    assert accepted["accepted"]
    assert _benchmark_status(cases, [], memory_acceptance=accepted) == "completed"

    cases[0]["tool_checks"][1]["passed"] = False
    accepted = _memory_acceptance(cases)
    assert not accepted["evidence_refs_valid"]
    assert _benchmark_status(cases, [], memory_acceptance=accepted) == "failed"


def test_memory_heldout_gate_rejects_thread_and_final_answer_leaks():
    cases = [
        {
            "case_id": str(index),
            "memory_ability": "memory_cross_session_reasoning",
            "passed": True,
            "tool_checks": [
                {"check": "activity_memory_selection", "passed": True},
                {"check": "activity_memory_evidence_refs", "passed": True},
                {"check": "resolved_activity_scope", "passed": True},
            ],
            "communication_checks": [
                {"check": "reply_contains:recorded fact", "passed": True},
            ],
        }
        for index in range(35)
    ]
    cases[0]["tool_checks"].append(
        {
            "check": "activity_thread_cross_task_isolation",
            "passed": False,
            "failures": [{"failure": "cross-activity text leak", "text": "FOREIGN-FACT"}],
        }
    )
    cases[1]["communication_checks"].append(
        {"check": "reply_excludes:FOREIGN-FACT", "passed": False}
    )

    accepted = _memory_acceptance(cases)

    assert accepted["cross_activity_leaks"] == 2
    assert accepted["accepted"] is False


def test_lisa_prompt_makes_activity_continuation_order_atomic():
    prompt = " ".join(LISA_SYSTEM_PROMPT.casefold().split())
    assert (
        "find_activity -> get_activity_thread -> resolve_chat_activity -> load_activity_memory"
        in prompt
    )
    assert "do not call todo_read, todo_write, list_recent_activities, or other tools between lookup, inspection, and resolution" in prompt
    assert "do not call get_activity_thread again just to" in prompt.lower()
    assert "use returned activity-memory evidence even when its status is `pending` or `fallback_truncated`" in prompt


def test_lisa_prompt_uses_minimal_flow_for_explicit_new_activity_requests():
    prompt = " ".join(LISA_SYSTEM_PROMPT.split()).lower()
    assert "when the user explicitly asks to create or track a new activity, do not search first" in prompt
    assert "only record that work in lisa's activity feed" in prompt
    assert "words such as \"investigate\" or \"agent\" in an activity title do not authorize" in prompt


def test_lisa_prompt_describes_paired_activity_timeline_fields():
    prompt = " ".join(LISA_SYSTEM_PROMPT.split()).lower()
    assert "pass both event_type and event_title to append a timeline action" in prompt


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


def test_context_benchmark_is_separate_and_covers_interleaving_threads_and_compaction():
    manifest, cases, digest = load_activity_dataset(
        DATASET.parent / "conversation_context" / "manifests" / "benchmark.json"
    )

    assert manifest.name == "lisa-conversation-context-agent-benchmark-2026-10"
    assert len(cases) == manifest.case_count == 2
    assert {case.case_id for case in cases} == {
        "main_chat_interleaves_tasks_after_compaction",
        "slack_threads_keep_context_separate",
    }
    assert all(case.split == "benchmark" for case in cases)
    assert any(
        len({turn.thread for turn in case.turns if turn.chat == "slack"}) > 1
        for case in cases
    )
    assert digest


def test_activity_memory_suite_has_pinned_70_case_source_port_and_explicit_memory_labels():
    manifest, cases, digest = load_activity_dataset(
        MEMORY_DATASET / "manifests/benchmark.json"
    )

    assert manifest.case_count == len(cases) == 70
    assert sum(case.split == "development" for case in cases) == 35
    assert sum(case.split == "benchmark" for case in cases) == 35
    assert set(manifest.source_revisions) == {"LongMemEval", "LongMemEval-V2"}
    assert set(manifest.source_checksums) == {
        "LongMemEval oracle input",
        "LongMemEval-V2 question input",
        "LongMemEval-V2 small haystack index",
        "LongMemEval-V2 trajectory text source",
    }
    assert all(case.force_compaction for case in cases)
    ported = [case for case in cases if case.upstream_source_id]
    assert len(ported) == 60
    assert all(case.upstream_benchmark and case.upstream_revision for case in ported)
    assert all(case.adaptation_note for case in cases)
    for ability in (
        "memory_information_extraction",
        "memory_cross_session_reasoning",
        "memory_temporal_reasoning",
        "memory_knowledge_updates",
        "memory_abstention",
        "memory_static_state_recall",
        "memory_dynamic_state_tracking",
        "memory_workflow_knowledge",
        "memory_environment_gotchas",
        "memory_premise_awareness",
    ):
        selected = [case for case in cases if case.category == ability]
        assert len(selected) == 6
        assert sum(case.split == "development" for case in selected) == 3
        assert sum(case.split == "benchmark" for case in selected) == 3
        assert all(case.upstream_source_id for case in selected)
        assert all(len(case.expected.activities) >= 2 for case in selected)
        assert all(
            any(turn.memory_activity_title_contains for turn in case.turns)
            for case in selected
        )
    adversarial = [case for case in cases if case.category == "memory_adversarial"]
    assert len(adversarial) == 10
    assert sum(case.split == "development" for case in adversarial) == 5
    assert sum(case.split == "benchmark" for case in adversarial) == 5
    assert {case.adversarial_type for case in adversarial} == {
        "ambiguous_reference",
        "corrected_fact",
        "archived_parent_child",
        "pending_snapshot",
        "cross_activity_abstention",
    }
    assert all(case.expected.activity_count == 3 for case in adversarial if case.adversarial_type in {
        "ambiguous_reference", "archived_parent_child"
    })
    assert any(case.defer_activity_memory_before_turns for case in adversarial)
    assert any(case.seed_archived_activities for case in adversarial)
    assert any(
        turn.expects_abstention
        for case in adversarial for turn in case.turns
    )
    assert any(
        turn.memory_activity_title_must_not_contain
        for case in adversarial for turn in case.turns
    )
    assert all(
        turn.memory_text_must_not_include
        for case in cases for turn in case.turns
        if turn.memory_activity_title_contains
        and turn.memory_activity_title_must_not_contain
    )
    assert all(
        {phrase.casefold() for phrase in turn.memory_text_must_not_include}
        <= {phrase.casefold() for phrase in turn.reply_must_not_include}
        for case in cases for turn in case.turns
        if turn.memory_activity_title_contains
    )
    assert all(
        any(turn.expects_abstention and turn.reply_must_not_include for turn in case.turns)
        for case in adversarial
        if case.adversarial_type in {"ambiguous_reference", "cross_activity_abstention"}
    )
    cross_activity = [
        case for case in adversarial if case.adversarial_type == "cross_activity_abstention"
    ]
    assert len(cross_activity) == 2
    assert all(
        len(case.turns[0].resolved_activity_title_contains) == 2
        and any("CUST-" in phrase for turn in case.turns for phrase in turn.memory_text_must_not_include)
        for case in cross_activity
    )
    assert digest


def test_memory_recall_rubrics_match_the_facts_the_case_actually_records():
    _, cases, _ = load_activity_dataset(MEMORY_DATASET / "manifests/benchmark.json")
    by_id = {case.case_id: case for case in cases}

    for suffix in ("01", "02", "03"):
        abstention = by_id[f"memory_abstention_ben_{suffix}"]
        date_turn = abstention.turns[3]
        assert "client launch date" in date_turn.text.casefold()
        assert not date_turn.expects_abstention
        assert date_turn.reply_must_include

        premise = by_id[f"memory_premise_awareness_ben_{suffix}"]
        recorded_id_turn = premise.turns[3]
        assert "approval id" in recorded_id_turn.text.casefold()
        assert "backup label" not in recorded_id_turn.text.casefold()
        assert not recorded_id_turn.expects_abstention
        assert recorded_id_turn.reply_must_include

        source_answer_turn = premise.turns[5]
        assert not source_answer_turn.expects_abstention
        assert source_answer_turn.reply_must_include

        development_abstention = by_id[f"memory_abstention_dev_{suffix}"]
        backup_turn = development_abstention.turns[3]
        assert f"MAPLE-{24 + int(suffix)}-P4" in backup_turn.reply_must_include
        assert "backup label" in backup_turn.text.casefold()


def test_heldout_memory_rubrics_accept_equivalent_answer_wording():
    _, cases, _ = load_activity_dataset(MEMORY_DATASET / "manifests/benchmark.json")
    by_id = {case.case_id: case for case in cases}

    # These source answers are factual recall checks. Their rubrics should
    # require the key facts, not one exact sentence or punctuation pattern.
    crash_answer = by_id["memory_environment_gotchas_ben_02"].turns[-1]
    assert [option.model_dump() for option in crash_answer.reply_must_match_exactly_one_of] == [
        {
            "must_include": ["crash is expected"],
            "must_not_include": [
                "not true that the crash is expected",
                "not the case that the crash is expected",
            ],
        },
        {
            "must_include": ["keep the default", "for text"],
            "must_not_include": [
                "do not keep the default",
                "don't keep the default",
                "never keep the default",
                "should not keep the default",
                "shouldn't keep the default",
            ],
        },
        {
            "must_include": ["directly open", "user's record"],
            "must_not_include": [
                "do not directly open",
                "don't directly open",
                "never directly open",
                "should not directly open",
                "shouldn't directly open",
            ],
        },
    ]
    checkout_answer = by_id["memory_environment_gotchas_ben_03"].turns[-1]
    assert set(checkout_answer.reply_must_include) >= {
        "Reorder",
        "Proceed to Checkout",
        "continue the normal checkout",
    }
    fitness_answer = by_id["memory_cross_session_reasoning_ben_03"].turns[-1]
    assert "4 days" in fitness_answer.reply_must_include
    extracted_terms = by_id["memory_information_extraction_ben_03"].turns[-1]
    assert set(extracted_terms.reply_must_include) >= {
        "sexual fixations",
        "problematic sexual behaviors",
        "sexual impulsivity",
        "compulsive sexuality",
    }


def test_ambiguous_activity_turn_tracks_clarification_without_loading_candidates():
    _, cases, _ = load_activity_dataset(MEMORY_DATASET / "manifests/benchmark.json")
    by_id = {case.case_id: case for case in cases}

    for case_id, candidates in (
        ("memory_adversarial_dev_01", {"Orchid work 22", "Maple work 22"}),
        ("memory_adversarial_ben_01", {"Orchid work 12", "Maple work 12"}),
    ):
        case = by_id[case_id]
        turn = case.turns[2]

        assert turn.expects_abstention
        assert not turn.forbid_activity_memory_load
        assert {
            "resolve_chat_activity",
            "load_activity_memory",
            "set_activity_progress",
        } <= set(turn.require_tools)
        assert turn.tool_order == [
            "resolve_chat_activity",
            "load_activity_memory",
            "set_activity_progress",
        ]
        assert any("clarification" in title.casefold() for title in turn.resolved_activity_title_contains)
        assert "which activity" in " ".join(turn.reply_must_include).casefold()
        assert candidates <= set(turn.memory_activity_title_must_not_contain)
        assert "get_activity_thread" in turn.forbid_tools
        assert all(
            value in turn.memory_text_must_not_include
            for value in turn.reply_must_not_include
        )
        assert case.expected.activity_count == 3
        assert any("clarification" in item.title_contains.casefold() for item in case.expected.activities)


def test_ambiguous_activity_scoring_rejects_candidate_thread_reads():
    turn = ActivityTurn(
        chat="main",
        text="Ask which activity I mean.",
        forbid_tools=["get_activity_thread"],
    )
    scored = _score_turn(
        turn,
        {
            "activities": [{"kind": "tool", "tool_name": "get_activity_thread", "arguments": {}}],
            "messages": [{"role": "assistant", "text": "Which activity do you mean?"}],
        },
        {},
        1,
    )

    check = next(item for item in scored["tool_checks"] if item["check"] == "forbidden_tool:get_activity_thread")
    assert not check["passed"]


def test_repeat_recorded_result_turns_keep_source_quotes_without_requiring_exact_punctuation():
    import re

    _, cases, _ = load_activity_dataset(MEMORY_DATASET / "manifests/benchmark.json")
    repeat_turns = [
        (case.case_id, index, turn)
        for case in cases
        for index, turn in enumerate(case.turns)
        if "repeat the recorded result briefly" in turn.text.casefold()
    ]

    assert repeat_turns
    for case_id, _, turn in repeat_turns:
        case = next(case for case in cases if case.case_id == case_id)
        match = re.search(
            r'the recorded answer text was "(.*?)"\. Background:',
            case.turns[0].text,
            re.DOTALL,
        )
        assert match, case_id
        assert turn.reply_must_quote == [match.group(1)]
        assert not _turn_requires_verbatim_answer(turn.text)


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
    assert independent.turns[0].tool_call_counts.get("complete_activity", 0) == 0
    assert independent.turns[0].tool_order == ["resolve_chat_activity", "resolve_chat_activity"]
    assert "lisa_db" not in independent.turns[0].require_tools
    assert independent.expected.activity_count == 2
    assert [item.status for item in independent.expected.activities] == ["success", "success"]
    timeout = by_id["activity_tracking_agent_timeout"].turns[1]
    assert timeout.require_tools.count("complete_activity") == 1
    assert timeout.tool_order[-1] == "complete_activity"
    assert by_id["independent_requests_get_separate_activities"].turns[0].tool_call_counts[
        "resolve_chat_activity"
    ] == 2
    assert by_id["clarify_ambiguous_activity_before_update"].expected.activity_count == 3
    assert [item.status for item in by_id[
        "clarify_ambiguous_activity_before_update"
    ].expected.activities] == ["blocked", "blocked", "blocked"]
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
    non_owner = by_id["reject_non_owner_policy_approval"]
    assert non_owner.turns[1].expected_tool_errors == ["resolve_chat_activity"]


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
            "policy_explicitly_deferred_approval",
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


def test_activity_benchmark_query_exclusion_ignores_non_query_tool_arguments():
    turn = ActivityTurn(
        chat="main",
        text="Inspect policy schema",
        tool_query_arguments_must_not_include=["archived_at"],
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "lisa_db",
                "arguments": {"query": "SELECT id FROM lisa.policies"},
            },
            {
                "kind": "tool",
                "tool_name": "update_activity",
                "arguments": {"details": {"archived_at_supported": False}},
            },
        ],
        "messages": [],
    }

    scored = _score_turn(turn, result, {}, 1)

    assert next(
        check for check in scored["tool_checks"]
        if check["check"] == "tool_query_arguments_exclude:archived_at"
    )["passed"]


def test_activity_benchmark_query_exclusion_rejects_column_probe():
    turn = ActivityTurn(
        chat="main",
        text="Inspect policy schema",
        tool_query_arguments_must_not_include=["archived_at"],
    )
    result = {
        "activities": [
            {
                "kind": "tool",
                "tool_name": "lisa_db",
                "arguments": {"query": "SELECT archived_at FROM lisa.policies"},
            }
        ],
        "messages": [],
    }

    scored = _score_turn(turn, result, {}, 1)

    assert not next(
        check for check in scored["tool_checks"]
        if check["check"] == "tool_query_arguments_exclude:archived_at"
    )["passed"]


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
    assert "archived_at" in case.turns[0].tool_query_arguments_must_not_include
    assert {
        "id", "scope", "target_agent_name", "name", "criteria", "approved",
        "created_at", "updated_at",
    } <= set(case.turns[0].tool_arguments_must_include)
    assert set(case.turns[0].reply_must_include) >= {
        "id", "scope", "target_agent_name", "name", "criteria", "approved",
        "created_at", "updated_at", "archived_at", "not stored",
    }


def test_activity_benchmark_defers_policy_when_owner_explicitly_says_not_to_approve():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    case = next(case for case in cases if case.case_id == "policy_explicitly_deferred_approval")

    assert "do not approve it yet" in case.turns[-1].text.casefold()
    assert "approve_policy" in case.turns[-1].forbid_tools
    assert case.expected.pending_policy_count == 1


def test_ambiguous_activity_case_checks_candidates_unchanged_and_waits_for_choice():
    _, cases, _ = load_activity_dataset(DATASET / "manifests/benchmark.json")
    expected_choices = {
        "clarify_ambiguous_activity_before_update": (
            "Maple 244", "Maple 245", "Maple 244 or Maple 245"
        ),
        "ambiguous_update_waits_for_owner_choice": (
            "Harbor 18", "Harbor 19", "Harbor 18 or Harbor 19"
        ),
    }
    for case_id, (first_title, second_title, choice) in expected_choices.items():
        case = next(case for case in cases if case.case_id == case_id)
        expectations = {item.title_contains: item for item in case.expected.activities}
        assert expectations[first_title].status == "blocked"
        assert expectations[second_title].status == "blocked"
        clarification = next(
            item for title, item in expectations.items() if title.startswith("Clarify")
        )
        assert clarification.next_action_contains == choice
        assert case.turns[-1].unchanged_activities == [first_title, second_title]


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

    # Namespace discovery and loading are OpenHands setup calls. They must not
    # make the activity-domain sequence appear out of order.
    result["activities"] = [
        {"kind": "tool", "tool_name": "discover_tool_namespaces"},
        {"kind": "tool", "tool_name": "load_tool_namespace"},
        {"kind": "tool", "tool_name": "find_activity"},
        {"kind": "tool", "tool_name": "get_activity_thread"},
        {"kind": "tool", "tool_name": "resolve_chat_activity"},
        {"kind": "tool", "tool_name": "load_activity_memory"},
        {"kind": "tool", "tool_name": "set_activity_progress"},
    ]
    continuation = ActivityTurn(
        chat="main",
        text="Resume the existing work",
        tool_order=[
            "find_activity",
            "get_activity_thread",
            "resolve_chat_activity",
            "load_activity_memory",
            "set_activity_progress",
        ],
    )
    scored = _score_turn(continuation, result, {}, 0)
    order_check = next(
        check for check in scored["tool_checks"]
        if check["check"].startswith("tool_order:")
    )
    assert order_check["passed"]
    assert order_check["actual"] == [
        "find_activity",
        "get_activity_thread",
        "resolve_chat_activity",
        "load_activity_memory",
        "set_activity_progress",
    ]

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


def test_multiactivity_projection_does_not_require_shared_messages_as_thread_actions():
    class ActivityStore:
        def get_activity_thread(self, activity_id):
            return {
                "references": [
                    {"reference_type": "message", "reference_id": "shared-user"},
                    {"reference_type": "message", "reference_id": "shared-assistant"},
                ],
                "actions": [
                    {
                        "seq": 1,
                        "event_type": "task.note",
                        "title": "Scoped note",
                        "created_at": "now",
                        "reference_type": None,
                        "reference_id": None,
                    }
                ],
            }

    result = {
        "messages": [
            {"id": "shared-user", "role": "user"},
            {"id": "shared-assistant", "role": "assistant"},
        ],
        "activities": [],
        "chat_activity_ids": ["activity-1", "activity-2"],
    }

    check = _score_trajectory_links(ActivityStore(), result, set())

    assert check["passed"]


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
    assert "the single-chat transcript may interleave activities" in prompt
    assert "never repeat another activity's facts" in prompt
    assert "do not query lisa_db or search sessions" in prompt
    assert "continuation in a later turn" in prompt
    assert "reuse that activity id" in prompt
    assert "call resolve_chat_activity with a concise task-only summary immediately before any other tool call" in prompt
    assert "route by the user's requested outcome" in prompt
    assert "call get_activity_thread next" in prompt
    assert "if the matching activity is closed, inspect its thread" in prompt
    assert "for a chat session evaluation, resolve the request and call evaluate_session" in prompt
    assert "the tool reuses the active resolved activity" in prompt
    assert "resolve each independent request immediately before doing its task-specific work" in prompt
    assert "resolve every request before the final reply" in prompt
    assert "never create a second activity just because the next action is approval" in prompt
    assert "keep a newly created or updated policy's activity in `waiting`" in prompt
    assert "when an authorized owner asks to approve a policy, call approve_policy with the policy id they named" in prompt
    assert "use list_policies and read_policy" in prompt
    assert "for an explicit request to inspect policy catalog rows or run sql diagnostics, lisa_db must read lisa.policies" in prompt
    assert "verify using an explicit zero-row projection from `lisa.policies`" in prompt
    assert "never probe a missing column by selecting it" in prompt
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
    assert "do not probe absent columns by selecting them" in database_schema
    assert ", ".join(POLICY_COLUMNS) in database_schema
    assert "truncated" in database_schema
    assert "wildcard projections are rejected" in database_schema
    assert "full ``lisa.<relation>`` name exactly as listed" in database_schema

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

    assert "Resolve that clarification activity, immediately load only its memory, set it to waiting" in prompt
    assert "Do not load either candidate activity's memory" in prompt
    assert "set it to waiting, and ask which candidate they mean" in activity_skill
    assert "read either candidate's thread with `get_activity_thread`" in activity_skill
    assert "load either candidate's memory" in activity_skill
    assert "are transient context lookups" in activity_skill
    assert "not written into activity history or snapshots" in activity_skill


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
        activity_store=SimpleNamespace(
            list_activity_feed=lambda query=None, **_: [
                item for item in feed
                if query is None or query.casefold() in item["title"].casefold()
            ]
        ),
        policy_service=SimpleNamespace(read_policy_records=list, list_policy_records=list),
    )

    checks = _score_state(services, labels.model_dump(exclude_unset=True))

    assert all(check["passed"] for check in checks)


def test_activity_label_can_require_an_archived_parent_link():
    labels = ActivityLabels.model_validate(
        {
            "activities": [
                {
                    "title_contains": "Pine 842 follow-up",
                    "status": "blocked",
                    "parent_title_contains": "Pine 842 original",
                }
            ]
        }
    )
    parent = _feed_row("parent-id", "Pine 842 original", "success")
    child = _feed_row("child-id", "Pine 842 follow-up", "blocked")
    child["parent_id"] = "parent-id"
    store = SimpleNamespace(
        list_activity_feed=lambda query=None, **_: [
            item for item in (parent, child)
            if query is None or query.casefold() in item["title"].casefold()
        ],
        get_activity_thread=lambda _: {"actions": []},
    )
    services = SimpleNamespace(
        activity_store=store,
        policy_service=SimpleNamespace(read_policy_records=list, list_policy_records=list),
    )

    passing = _score_state(services, labels.model_dump(exclude_unset=True))
    child["parent_id"] = "different-parent-id"
    failing = _score_state(services, labels.model_dump(exclude_unset=True))

    assert passing[-1]["passed"] is True
    assert failing[-1]["passed"] is False


def test_activity_scope_check_rejects_writes_to_a_different_resolved_task():
    class Store:
        def list_activity_feed(self, query=None, **_kwargs):
            activities = [
                _feed_row("orchid-id", "Orchid release"),
                _feed_row("maple-id", "Maple migration"),
            ]
            return [
                item for item in activities
                if query is None or query.casefold() in item["title"].casefold()
            ]

    turn = ActivityTurn(
        chat="main",
        text="Update the Orchid release.",
        resolved_activity_title_contains=["Orchid release"],
    )
    result = {
        "chat_activity_ids": ["orchid-id"],
        "activities": [
            {
                "kind": "tool",
                "tool_name": "set_activity_progress",
                "arguments": {"activity_id": "maple-id"},
            }
        ],
    }

    check = _score_resolved_activity_scope(turn, result, Store())

    assert not check["passed"]
    assert check["wrong_write_targets"] == ["maple-id"]


def test_activity_scope_check_rejects_wrong_write_on_recall_without_title_rubric():
    turn = ActivityTurn(
        chat="main",
        text="Recall the Orchid release note.",
    )
    result = {
        "chat_activity_ids": ["orchid-id"],
        "activities": [
            {
                "kind": "tool",
                "tool_name": "set_activity_progress",
                "arguments": {"activity_id": "maple-id"},
            }
        ],
    }

    check = _score_resolved_activity_scope(turn, result, activity_store=None)

    assert check is not None
    assert check["passed"] is False
    assert check["wrong_write_targets"] == ["maple-id"]


def test_activity_snapshot_detects_lifecycle_or_timeline_changes():
    from benchmark.activity_runner import _activity_snapshot, _compare_activity_snapshots

    activity = _feed_row(
        "00000000-0000-4000-8000-000000000006", "Maple 244 retry", "blocked"
    )
    actions = [{"seq": 1, "event_type": "accepted", "summary": "Opened"}]
    store = SimpleNamespace(
        list_activity_feed=lambda **_: [activity],
        get_activity_thread=lambda _: {"actions": actions},
    )
    before = _activity_snapshot(store, ["Maple 244"])

    assert _compare_activity_snapshots(before, _activity_snapshot(store, ["Maple 244"]))["passed"]

    actions.append({"seq": 2, "event_type": "progress", "summary": "Changed"})
    changed = _compare_activity_snapshots(before, _activity_snapshot(store, ["Maple 244"]))

    assert not changed["passed"]


def test_activity_snapshot_compares_original_candidate_ids_after_new_similar_activity():
    from benchmark.activity_runner import _activity_snapshot, _compare_activity_snapshots

    original = _feed_row(
        "00000000-0000-4000-8000-000000000007", "Harbor 19 checkout retry failure", "blocked"
    )
    store = SimpleNamespace(
        list_activity_feed=lambda **_: [original],
        get_activity_thread=lambda _: {"actions": []},
    )
    before = _activity_snapshot(store, ["Harbor 19"])

    clarification = _feed_row(
        "00000000-0000-4000-8000-000000000008",
        "Clarification: Harbor 18 vs Harbor 19 checkout retry failure",
        "blocked",
    )
    store.list_activity_feed = lambda **_: [original, clarification]

    after = _activity_snapshot(store, ["Harbor 19"], existing_snapshot=before)

    assert _compare_activity_snapshots(before, after)["passed"]
