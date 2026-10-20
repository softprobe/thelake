"""Unit tests for production-path policy evaluation metric arithmetic."""

from benchmark.policy_runner import _summary


def test_metrics_count_misses_false_alarms_and_uncertainty_separately():
    rows = [
        {
            "expected": "violation",
            "escalated": True,
            "verdict": "fail",
        },
        {
            "expected": "violation",
            "escalated": False,
            "verdict": None,
        },
        {
            "expected": "compliant",
            "escalated": True,
            "verdict": "pass",
        },
        {
            "expected": "not_applicable",
            "escalated": False,
            "verdict": None,
        },
        {
            "expected": "insufficient_evidence",
            "escalated": True,
            "verdict": "uncertain",
        },
    ]
    metrics = _summary(rows)
    assert metrics["triage_confusion"] == {"tp": 1, "fn": 1, "fp": 1, "tn": 1}
    assert metrics["violation_recall"] == 0.5
    assert metrics["triage_accuracy"] == 0.5
    assert metrics["insufficient_evidence_count"] == 1
    assert metrics["uncertain_escalation_recall"] == 1
    assert metrics["end_to_end_violation_recall"] == 0.5
    assert metrics["end_to_end_accuracy"] == 0.75
    assert metrics["end_to_end_precision"] == 1


def test_metrics_do_not_claim_perfect_recall_without_positives():
    metrics = _summary(
        [
            {"expected": "compliant", "escalated": False, "verdict": None},
        ]
    )
    assert metrics["violation_recall"] is None
    assert metrics["end_to_end_violation_recall"] is None


def test_empty_report_is_undefined():
    assert _summary([])["triage_accuracy"] is None


def test_final_false_alerts_include_out_of_scope_pairs():
    metrics = _summary(
        [
            {"expected": "violation", "escalated": True, "verdict": "fail"},
            {"expected": "not_applicable", "escalated": True, "verdict": "fail"},
        ]
    )
    assert metrics["end_to_end_precision"] == 0.5
    assert metrics["end_to_end_accuracy"] == 0.5
    assert metrics["end_to_end_detection_confusion"]["fp"] == 1


def test_triage_miss_on_violation_is_end_to_end_miss():
    metrics = _summary(
        [
            {"expected": "violation", "escalated": False, "verdict": None},
        ]
    )
    assert metrics["end_to_end_violation_recall"] == 0
    assert metrics["end_to_end_detection_confusion"]["fn"] == 1
