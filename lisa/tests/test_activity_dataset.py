"""Integrity and deterministic scoring contracts for the agent benchmark dataset."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from lisa.evaluation.activity_benchmark import _interpolate, _rate, _score_state, _score_turn
from lisa.evaluation.activity_dataset import ActivityLabels, ActivityTurn, load_activity_dataset

DATASET = Path(__file__).resolve().parents[1] / "datasets/activity_management"


def test_activity_benchmark_release_has_frozen_cases_and_both_splits():
    manifest, cases, digest = load_activity_dataset(DATASET / "manifests/benchmark.json")

    assert manifest.case_count == len(cases) == 16
    assert digest
    assert {case.split for case in cases} == {"development", "test"}
    assert {case.category for case in cases} >= {
        "create",
        "update",
        "continuity",
        "policy",
        "followup",
    }
    assert len({case.case_id for case in cases}) == len(cases)


def test_activity_benchmark_rejects_changed_case_bytes(tmp_path):
    copied = tmp_path / "activity_management"
    (copied / "manifests").mkdir(parents=True)
    (copied / "cases.jsonl").write_text((DATASET / "cases.jsonl").read_text() + "\n")
    (copied / "manifests/benchmark.json").write_text(
        (DATASET / "manifests/benchmark.json").read_text()
    )

    with pytest.raises(ValueError, match="digest mismatch"):
        load_activity_dataset(copied / "manifests/benchmark.json")


def test_activity_benchmark_score_checks_real_tool_trace_and_reply_rubric():
    turn = ActivityTurn(
        chat="slack",
        text="Confirm {proposal_id}",
        require_tools=["apply_policy_proposal"],
        forbid_tools=["create_policy_proposal"],
        reply_must_include=["$proposal_id", "refund"],
        reply_must_not_include=["I already changed it"],
    )
    result = {
        "activities": [
            {"kind": "tool", "tool_name": "apply_policy_proposal"},
            {"kind": "tool", "tool_name": "list_pending_policy_proposals"},
        ],
        "messages": [{"role": "assistant", "text": "Applied proposal abc for the refund rule."}],
    }

    scored = _score_turn(turn, result, {"proposal_id": "abc"}, 1)

    assert all(item["passed"] for item in scored["tool_checks"] + scored["communication_checks"])
    assert _interpolate("Confirm {proposal_id}", {"proposal_id": "abc"}) == "Confirm abc"


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


def test_activity_benchmark_rate_preserves_empty_denominator():
    assert _rate([]) is None
    assert _rate([{"passed": True}, {"passed": False}]) == 0.5


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
            list_policy_proposals=lambda **_: [],
        ),
        policy_service=SimpleNamespace(read_policy_records=list),
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
