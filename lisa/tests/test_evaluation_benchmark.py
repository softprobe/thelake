"""Metric arithmetic and shared dataset-to-provider execution contracts."""

import importlib.util
from dataclasses import replace
from pathlib import Path

import pytest
from test_policy_selection import FakeDecisionsClient

from lisa.evaluation.dataset import load_dataset
from lisa.evaluation.policy_selection import OpenAIPolicySelector

spec = importlib.util.spec_from_file_location(
    "evaluation_benchmark", Path(__file__).resolve().parents[1] / "scripts/evaluate_policies.py"
)


def benchmark():
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def subset(source="synthetic", incomplete=False):
    data = load_dataset()
    batches = [
        b
        for b in data.batches(source=source)
        if data.trajectories[b.trajectory_ref].trajectory.complete != incomplete
    ]
    return replace(
        data,
        manifest=data.manifest.model_copy(
            update={"batches": batches if incomplete else batches[:1]}
        ),
    )


def test_metrics_count_misses_false_alarms_and_uncertainty_separately():
    rows = [
        {"expected": "violation", "decision": "suspected_violation", "verdict": "fail"},
        {"expected": "violation", "decision": "no_violation_found", "verdict": None},
        {"expected": "compliant", "decision": "suspected_violation", "verdict": "pass"},
        {"expected": "not_applicable", "decision": "no_violation_found", "verdict": None},
        {
            "expected": "insufficient_evidence",
            "decision": "insufficient_evidence",
            "verdict": "uncertain",
        },
    ]
    metrics = benchmark().summarize(rows)
    assert metrics["triage_confusion"] == {"tp": 1, "fn": 1, "fp": 1, "tn": 1}
    assert metrics["violation_recall"] == 0.5
    assert metrics["triage_accuracy"] == 0.5
    assert metrics["escalation_rate"] == 0.6
    assert metrics["insufficient_evidence_count"] == 1
    assert metrics["end_to_end_violation_recall"] == 0.5
    assert metrics["end_to_end_detection_accuracy"] == 0.75
    assert metrics["end_to_end_precision"] == 1


def test_metrics_do_not_claim_perfect_recall_without_positives_or_judge():
    metrics = benchmark().summarize(
        [
            {"expected": "compliant", "decision": "no_violation_found", "verdict": None},
        ]
    )
    assert metrics["violation_recall"] is None
    assert metrics["end_to_end_violation_recall"] is None


def test_empty_report_is_undefined():
    assert benchmark().summarize([])["triage_accuracy"] is None


def test_final_false_alerts_include_out_of_scope_pairs():
    metrics = benchmark().summarize(
        [
            {"expected": "violation", "decision": "suspected_violation", "verdict": "fail"},
            {"expected": "not_applicable", "decision": "suspected_violation", "verdict": "fail"},
        ]
    )
    assert metrics["end_to_end_precision"] == 0.5
    assert metrics["end_to_end_detection_accuracy"] == 0.5
    assert metrics["end_to_end_detection_confusion"]["fp"] == 1


def test_enabled_judge_with_all_violations_skipped_has_zero_recall():
    metrics = benchmark().summarize(
        [
            {"expected": "violation", "decision": "no_violation_found", "verdict": None},
        ],
        judge_enabled=True,
    )
    assert metrics["end_to_end_violation_recall"] == 0
    assert metrics["end_to_end_detection_confusion"]["fn"] == 1


def test_runs_every_catalog_pair_and_judges_only_escalations():
    data = subset()
    selector = OpenAIPolicySelector(
        client=FakeDecisionsClient(
            ["suspected_violation", "insufficient_evidence", *(["no_violation_found"] * 4)]
        )
    )
    requests = []

    def judge(request):
        requests.append(request)
        return {"status": "fail" if request["policy"]["name"] == "Refund approval" else "pass"}

    report = benchmark().run(data, selector, judge)
    assert len(report["rows"]) == 6
    assert len(requests) == 2
    assert report["metrics"]["violation_recall"] == 1
    assert report["metrics"]["end_to_end_violation_recall"] == 1
    assert sum(row["verdict"] is None for row in report["rows"]) == 4
    assert all(request["trajectory"] == requests[0]["trajectory"] for request in requests)


@pytest.mark.parametrize("status", ["error", "invalid", None])
def test_aborts_judge_execution_failures(status):
    selector = OpenAIPolicySelector(
        client=FakeDecisionsClient(["suspected_violation", *(["no_violation_found"] * 5)])
    )
    with pytest.raises(RuntimeError, match="Judge failed"):
        benchmark().run(subset(), selector, lambda _: {"status": status})


def test_capture_gate_stops_both_providers_and_is_not_scored():
    class NoProviderCalls:
        model = "unused"

        def select(self, *_):
            pytest.fail("Incomplete capture must stop before triage")

    report = benchmark().run(
        subset(incomplete=True), NoProviderCalls(), lambda _: pytest.fail("No judge call")
    )
    assert report["metrics"]["pair_count"] == 0
    assert report["capture_gate"] == {"case_count": 4, "pair_count": 24}
    assert all(row["gate_status"] == "insufficient_evidence" for row in report["rows"])


def test_sparse_and_proposed_pairs_do_not_enter_accuracy():
    data = subset(source="state_bench")
    selector = OpenAIPolicySelector(client=FakeDecisionsClient(["no_violation_found"] * 10))
    report = benchmark().run(data, selector, mode="triage")
    assert len(report["rows"]) == 10
    assert report["coverage"]["unlabeled_pair_count"] == 9
    assert report["coverage"]["proposed_pair_count"] == 1
    assert report["coverage"]["scored_pair_count"] == 0
    assert report["metrics"]["triage_accuracy"] is None
    assert report["workload"]["eligible_pair_count"] == 10
    assert report["workload"]["escalation_rate"] == 0


def test_judge_only_mode_does_not_claim_triage_recall():
    report = benchmark().run(subset(), judge=lambda _: {"status": "pass"}, mode="judge")
    assert report["metrics"]["triage_accuracy"] is None
    assert report["metrics"]["violation_recall"] is None
    assert report["metrics"]["end_to_end_violation_recall"] == 0
    assert report["workload"]["judge_count"] == 6


def test_optional_ordering_flags_use_validated_defaults():
    data = subset()
    record = next(iter(data.trajectories.values()))
    payload = record.trajectory.model_dump(mode="json", exclude_unset=True)
    payload.pop("tool_order_certain")
    payload.pop("message_order_certain")
    from lisa.evaluation.contracts import Trajectory

    updated = record.model_copy(update={"trajectory": Trajectory.model_validate(payload)})
    data = replace(data, trajectories={**data.trajectories, record.ref: updated})
    selector = OpenAIPolicySelector(client=FakeDecisionsClient(["no_violation_found"] * 6))
    report = benchmark().run(data, selector, mode="triage")
    assert report["capture_gate"]["pair_count"] == 0
    assert report["metrics"]["pair_count"] == 6
