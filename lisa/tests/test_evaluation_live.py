"""Opt-in real-model dataset calibration, reported independently for every case."""

from __future__ import annotations

import os

import pytest
from lisa_benchmarks.dataset import load_dataset

from lisa.evaluation.contracts import EvaluationRequest
from lisa.evaluation.judge import run_evaluation

DATA = load_dataset()
PAIRS = [p for p in DATA.pairs() if p.annotation.outcome != "not_applicable"]
LIVE_ENABLED = os.getenv("LISA_RUN_EVALUATION_LIVE") == "1"
HAS_GEMINI_KEY = bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"))


def assert_pair_verdict(pair, verdict):
    expected = {
        "violation": {"fail"},
        "compliant": {"pass"},
        "insufficient_evidence": {"uncertain", "insufficient_evidence"},
        "not_applicable": {"pass", "uncertain"},
    }[pair.annotation.outcome]
    assert verdict["status"] in expected, (
        f"{pair.trajectory.name}: expected {pair.annotation.outcome}, received {verdict}"
    )
    assert verdict["rationale"].strip()
    if verdict["status"] in {"pass", "fail"}:
        references = verdict.get("references", [])
        assert references
        captured = {e.span_id for e in pair.trajectory.trajectory.events}
        assert all(reference["span_id"] in captured for reference in references)
    assert not any("prompt_event_body_truncated" in item for item in verdict["limitations"])


@pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_GEMINI_KEY),
    reason="Live dataset judge quality requires LISA_RUN_EVALUATION_LIVE=1 and Gemini credentials",
)
@pytest.mark.parametrize(
    "pair", PAIRS, ids=lambda pair: f"{pair.trajectory.provenance['source']}:{pair.trajectory.name}"
)
def test_pair_judge_calibration(pair):
    """Calibrate against explicit annotations; proposed labels are author expectations."""
    request = EvaluationRequest.model_validate(pair.request())
    verdict = run_evaluation(request.model_dump(mode="json", exclude_unset=True))
    assert_pair_verdict(pair, verdict)
