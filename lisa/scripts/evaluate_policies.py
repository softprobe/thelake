"""Opt-in model calibration; writes per-pair evidence and metrics to a local report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lisa.evaluation.dataset import DEFAULT_MANIFEST, load_dataset
from lisa.evaluation.judge import run_evaluation
from lisa.evaluation.policy_triage import OpenAIPolicyTriager
from lisa.models.policies import PolicyRule


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def summarize(rows, judge_enabled=None, *, triage_enabled=True):
    resolved = [r for r in rows if r["expected"] != "insufficient_evidence"]
    counts = dict.fromkeys(("tp", "fn", "fp", "tn"), 0)
    for row in resolved:
        positive = row["expected"] == "violation"
        escalated = row["decision"] != "no_violation_found"
        counts[("tp" if escalated else "fn") if positive else ("fp" if escalated else "tn")] += 1
    final_counts = dict.fromkeys(("tp", "fn", "fp", "tn"), 0)
    for row in resolved:
        positive = row["expected"] == "violation"
        detected = row.get("verdict") == "fail"
        final_counts[
            ("tp" if detected else "fn") if positive else ("fp" if detected else "tn")
        ] += 1
    judged = [r for r in rows if r.get("verdict") is not None]
    judge_enabled = bool(judged) if judge_enabled is None else judge_enabled
    judged_resolved = [r for r in judged if r["expected"] in {"compliant", "violation"}]
    metrics = {
        "pair_count": len(rows),
        "triage_confusion": counts,
        "violation_recall": ratio(counts["tp"], counts["tp"] + counts["fn"]),
        "triage_precision": ratio(counts["tp"], counts["tp"] + counts["fp"]),
        "triage_accuracy": ratio(counts["tp"] + counts["tn"], len(resolved)),
        "escalation_rate": ratio(
            sum(r["decision"] != "no_violation_found" for r in rows), len(rows)
        ),
        "insufficient_evidence_count": len(rows) - len(resolved),
        "uncertain_escalation_recall": ratio(
            sum(
                r["expected"] == "insufficient_evidence" and r["decision"] != "no_violation_found"
                for r in rows
            ),
            len(rows) - len(resolved),
        ),
        "judge_count": len(judged),
        "end_to_end_detection_confusion": final_counts if judge_enabled else None,
        "end_to_end_precision": ratio(final_counts["tp"], final_counts["tp"] + final_counts["fp"])
        if judge_enabled
        else None,
        "end_to_end_detection_accuracy": ratio(
            final_counts["tp"] + final_counts["tn"], len(resolved)
        )
        if judge_enabled
        else None,
        "judge_accuracy_on_escalated_resolved_pairs": ratio(
            sum(
                r["verdict"] == {"violation": "fail", "compliant": "pass"}[r["expected"]]
                for r in judged_resolved
            ),
            len(judged_resolved),
        ),
        "end_to_end_violation_recall": ratio(
            sum(r["expected"] == "violation" and r.get("verdict") == "fail" for r in rows),
            counts["tp"] + counts["fn"],
        )
        if judge_enabled
        else None,
    }

    if not triage_enabled:
        for key in (
            "triage_confusion",
            "violation_recall",
            "triage_precision",
            "triage_accuracy",
            "escalation_rate",
            "uncertain_escalation_recall",
        ):
            metrics[key] = None
    return metrics


def run(data, triager=None, judge=None, *, split=None, mode="pipeline"):
    """Present every catalog policy; score only explicitly adjudicated pairs."""
    if mode not in {"triage", "judge", "pipeline"}:
        raise ValueError("Unknown evaluation mode")
    if mode != "judge" and triager is None:
        raise ValueError("Triage requires a triager")
    if mode != "triage" and judge is None:
        raise ValueError("Judge mode requires a judge")
    rows = []
    for batch in data.batches(split=split):
        record = data.trajectories[batch.session_id]
        trajectory = record.trajectory.model_dump(mode="json", exclude_unset=True)
        annotations = {p.policy.policy_id: p.annotation for p in data.pairs(batch)}
        gated = not (
            record.trajectory.complete
            and record.trajectory.tool_order_certain
            and record.trajectory.message_order_certain
        )
        rules = [
            PolicyRule(id=policy_id, **data.policies[policy_id].policy.model_dump())
            for policy_id in batch.policy_ids
        ]
        decisions = {}
        if not gated and mode != "judge":
            decisions = {
                d["id"]: d["decision"]
                for d in triager.triage(trajectory, rules)["policy_decisions"]
            }
            if set(decisions) != set(batch.policy_ids):
                raise RuntimeError("Triager did not answer every catalog policy")
        for rule in rules:
            annotation = annotations.get(rule.id)
            decision = decisions.get(rule.id)
            verdict = None
            if (
                not gated
                and mode != "triage"
                and (mode == "judge" or decision != "no_violation_found")
            ):
                verdict = judge(
                    {
                        "run_id": f"{record.session_id}:{rule.id}",
                        "policy": data.policies[rule.id].policy.model_dump(mode="json"),
                        "trajectory": trajectory,
                    }
                )
                if verdict.get("status") not in {
                    "pass",
                    "fail",
                    "uncertain",
                    "insufficient_evidence",
                }:
                    raise RuntimeError(
                        f"Judge failed for {record.session_id}:{rule.id}: {verdict.get('status')}"
                    )
            rows.append(
                {
                    "session_id": record.session_id,
                    "policy_id": rule.id,
                    "annotation_status": annotation.status if annotation else None,
                    "expected": annotation.outcome if annotation else None,
                    "scored": annotation is not None and annotation.status == "adjudicated",
                    "split": batch.split,
                    "decision": decision,
                    "verdict": verdict["status"] if verdict else None,
                    "judge_result": verdict,
                    "gate_status": "insufficient_evidence" if gated else None,
                }
            )
    eligible = [r for r in rows if not r["gate_status"]]
    scored = [r for r in eligible if r["scored"]]
    gated = [r for r in rows if r["gate_status"]]
    return {
        "manifest": data.manifest.name,
        "manifest_digest": data.digest,
        "triage_model": triager.model if triager is not None else None,
        "mode": mode,
        "split": split,
        "coverage": {
            "presented_pair_count": len(rows),
            "unlabeled_pair_count": sum(r["annotation_status"] is None for r in rows),
            "proposed_pair_count": sum(r["annotation_status"] == "proposed" for r in rows),
            "disputed_pair_count": sum(r["annotation_status"] == "disputed" for r in rows),
            "scored_pair_count": len(scored),
        },
        "capture_gate": {
            "case_count": len({r["session_id"] for r in gated}),
            "pair_count": len(gated),
        },
        "workload": {
            "eligible_pair_count": len(eligible),
            "escalation_rate": ratio(
                sum(r["decision"] != "no_violation_found" for r in eligible), len(eligible)
            )
            if mode != "judge"
            else None,
            "judge_count": sum(r["verdict"] is not None for r in rows),
        },
        "metrics": summarize(scored, mode != "triage", triage_enabled=mode != "judge"),
        "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--split", choices=["development", "test"])
    parser.add_argument("--mode", choices=["triage", "judge", "pipeline"], default="pipeline")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--live", action="store_true", help="Allow paid provider calls")
    args = parser.parse_args()
    data = load_dataset(args.manifest)
    if not args.live:
        parser.error("--live is required to authorize provider calls")
    triager = OpenAIPolicyTriager() if args.mode != "judge" else None
    try:
        report = run(
            data,
            triager,
            run_evaluation if args.mode != "triage" else None,
            split=args.split,
            mode=args.mode,
        )
    finally:
        if triager is not None and triager.client is not None:
            triager.client.close()
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["metrics"], indent=2))


if __name__ == "__main__":
    main()
