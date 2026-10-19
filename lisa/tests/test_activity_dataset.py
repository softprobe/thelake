"""Integrity and deterministic scoring contracts for the agent benchmark dataset."""

from __future__ import annotations

from pathlib import Path

import pytest

from lisa.evaluation.activity_benchmark import _interpolate, _rate, _score_turn
from lisa.evaluation.activity_dataset import ActivityTurn, load_activity_dataset

DATASET = Path(__file__).resolve().parents[1] / "datasets/activity_management"


def test_activity_benchmark_release_has_frozen_cases_and_both_splits():
    manifest, cases, digest = load_activity_dataset(DATASET / "manifests/benchmark.json")

    assert manifest.case_count == len(cases) == 8
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


def test_activity_benchmark_rate_preserves_empty_denominator():
    assert _rate([]) is None
    assert _rate([{"passed": True}, {"passed": False}]) == 0.5
