from __future__ import annotations

import json
import runpy
from pathlib import Path

from thelake_evaluation_runner.contracts import EvaluationRequest


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "dogfood" / "connected_itinerary.json"
VALIDATE_RESPONSE = runpy.run_path(
    ROOT / "scripts" / "run_policy_dogfood.py"
)["validate_response"]


def test_policy_dogfood_cases_match_runner_contract_and_have_opposite_controls():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    evaluator = fixture["evaluator"]
    statuses = {}

    for case in fixture["cases"]:
        request = EvaluationRequest.model_validate(
            {
                "run_id": f"fixture:{case['case_id']}",
                "evaluator": evaluator,
                "evidence": case["evidence"],
            }
        )
        statuses[case["case_id"]] = case["expected_status"]
        assert request.evidence.trace_id == case["evidence"]["trace_id"]

    assert statuses == {
        "connected-disclosure-before-confirmation": "pass",
        "cancellation-without-disclosure": "fail",
        "incomplete-trace-is-not-a-pass": "insufficient_evidence",
    }


def test_policy_dogfood_cases_capture_warning_and_action_order():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cases = {case["case_id"]: case for case in fixture["cases"]}
    events = cases["connected-disclosure-before-confirmation"]["evidence"]["events"]
    positions = {event["span_id"]: event["sequence"] for event in events}
    assert positions["pass-disclosure"] < positions["pass-confirmation"]
    assert positions["pass-confirmation"] < positions["pass-cancel"]

    events = cases["cancellation-without-disclosure"]["evidence"]["events"]
    positions = {event["span_id"]: event["sequence"] for event in events}
    assert positions["fail-cancel"] < positions["fail-final"]
    assert "same-day onward flight" in next(
        event["content"] for event in events if event["span_id"] == "fail-final"
    )


def test_live_dogfood_response_checks_trace_identity_and_score_bounds():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    case = fixture["cases"][0]
    good_response = {
        "status": "pass",
        "trace_id": case["evidence"]["trace_id"],
        "score": 0.95,
        "evidence": [{"span_id": "pass-disclosure"}],
    }
    assert VALIDATE_RESPONSE(case, good_response) == []

    bad_response = {
        **good_response,
        "trace_id": "another-trace",
        "score": 1.2,
    }
    assert len(VALIDATE_RESPONSE(case, bad_response)) == 2
    bad_response = {**good_response, "evidence": [{"kind": "assistant_message"}]}
    assert VALIDATE_RESPONSE(case, bad_response) == [
        "pass/fail verdict must include valid evidence span references"
    ]
