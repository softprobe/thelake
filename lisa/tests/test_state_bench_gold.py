from __future__ import annotations

import json
from pathlib import Path

from lisa.evaluation.contracts import EvaluationRequest


FIXTURE = Path(__file__).parent / "fixtures" / "state_bench_gold.jsonl"


def test_state_bench_gold_cases_are_distinct_readable_and_replayable():
    cases = [
        json.loads(line)
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert len(cases) == 5
    assert len({case["source"]["task_id"] for case in cases}) == 5
    assert len({case["input"]["policy"]["name"] for case in cases}) == 5

    for case in cases:
        assert "provenance" not in case
        assert case["name"]
        assert case["category"] == "policy-compliance"
        assert case["tests"]
        assert {
            "benchmark",
            "benchmark_commit",
            "task_id",
            "source_file",
            "policy_source",
            "policy_approval",
            "evidence_source",
        } <= case["source"].keys()
        assert case["expected"]["verdict"] in {"pass", "fail", "uncertain"}
        assert case["expected"]["rationale"]
        assert case["expected"]["supporting_span"]

        request = EvaluationRequest.model_validate(case["input"])
        span_ids = {event.span_id for event in request.trajectory.events}
        assert case["expected"]["supporting_span"] in span_ids
