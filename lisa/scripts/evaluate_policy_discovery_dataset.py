"""Run the labeled policy-discovery provider evaluation with real Gemini calls."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import Counter
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.policy_discovery_scoring import score_policy_consolidation_case
from lisa.context import WorkspaceContext
from lisa.evaluation.policy_discovery import (
    analyze_policy_gap,
    synthesize_policy_suggestions,
)
from lisa.llm_recording import recording_scope

DATASET = Path(__file__).resolve().parents[1] / "datasets" / "policy_discovery" / "cases.jsonl"
CONSOLIDATION_DATASET = (
    Path(__file__).resolve().parents[1]
    / "datasets" / "policy_discovery" / "consolidation_cases.jsonl"
)


def _records(directory: Path) -> tuple[int, int]:
    input_tokens = output_tokens = 0
    for path in directory.glob("*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if row.get("type") != "response":
                continue
            usage = row.get("response", {}).get("usage", {}) or {}
            input_tokens += int(usage.get("prompt_token_count") or 0)
            output_tokens += int(usage.get("candidates_token_count") or 0)
    return input_tokens, output_tokens


def run(output: Path) -> dict:
    if not (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")):
        raise RuntimeError("Set GOOGLE_API_KEY or GEMINI_API_KEY for the live evaluation")
    cases = [json.loads(line) for line in DATASET.read_text(encoding="utf-8").splitlines() if line.strip()]
    consolidation_cases = [
        json.loads(line)
        for line in CONSOLIDATION_DATASET.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    dataset_hash = sha256(DATASET.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="lisa-policy-discovery-") as scratch:
        data_dir = Path(scratch)
        context = WorkspaceContext(
            workspace=str(uuid4()), data_dir=data_dir,
            agent_workspace_root=data_dir / "agent-workspace",
        )
        os.environ["LISA_LLM_TRACE_ENABLED"] = "1"
        rows = []
        with recording_scope(context, evaluation="policy_discovery"):
            for case in cases:
                signal = analyze_policy_gap(case["trajectory"])
                known = {event["span_id"] for event in case["trajectory"]["events"]}
                valid = all(span_id in known for span_id in signal.span_ids)
                rows.append({
                    "case_id": case["case_id"],
                    "expected_detected": case["expected_detected"],
                    "detected": signal.detected,
                    "correct": signal.detected == case["expected_detected"],
                    "citations_valid": valid,
                    "span_ids": signal.span_ids,
                    "summary": signal.summary,
                })
            consolidation_results = []
            for case in consolidation_cases:
                proposals = synthesize_policy_suggestions(
                    case["signals"], case["approved_policies"], case["owner_decisions"]
                )
                scored = score_policy_consolidation_case(case["expected"], proposals)
                scored["case_id"] = case["case_id"]
                scored["checks"] = case["expected"]["checks"]
                if "source_cases" in case:
                    scored["source_cases"] = case["source_cases"]
                scored["suggestions"] = [item.model_dump(mode="json") for item in proposals]
                consolidation_results.append(scored)
        input_tokens, output_tokens = _records(context.home() / "llm-calls")
    tp = sum(row["expected_detected"] and row["detected"] for row in rows)
    fp = sum(not row["expected_detected"] and row["detected"] for row in rows)
    fn = sum(row["expected_detected"] and not row["detected"] for row in rows)
    check_categories = sorted({
        check for case in consolidation_results for check in case["checks"]
    })
    category_accuracy = {}
    for category in check_categories:
        selected = [case for case in consolidation_results if category in case["checks"]]
        category_accuracy[category] = {
            "correct": sum(case["correct"] for case in selected),
            "support": len(selected),
            "accuracy": sum(case["correct"] for case in selected) / len(selected),
        }
    duplicate_counts = []
    for case in consolidation_results:
        duplicate_counts.extend(
            Counter(
                (item["name"].casefold().strip(), " ".join(item["criteria"].casefold().split()))
                for item in case["suggestions"]
            ).values()
        )
    duplicate_suggestion_count = sum(count - 1 for count in duplicate_counts)
    proposal_count = sum(case["suggestion_count"] for case in consolidation_results)
    consolidation_correct = sum(case["correct"] for case in consolidation_results)
    report = {
        "dataset": str(DATASET.relative_to(DATASET.parents[2])),
        "dataset_sha256": dataset_hash,
        "consolidation_dataset": str(CONSOLIDATION_DATASET.relative_to(CONSOLIDATION_DATASET.parents[2])),
        "consolidation_dataset_sha256": sha256(CONSOLIDATION_DATASET.read_bytes()).hexdigest(),
        "model": os.getenv("GEMINI_MODEL_NAME", "gemini-3.8-flash"),
        "cases": rows,
        "metrics": {
            "support": len(rows), "true_positive": tp, "false_positive": fp,
            "false_negative": fn,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "citation_invalid_count": sum(not row["citations_valid"] for row in rows),
            "consolidation_support": len(consolidation_results),
            "consolidation_correct": consolidation_correct,
            "consolidation_accuracy": consolidation_correct / len(consolidation_results),
            "proposal_count": proposal_count,
            "duplicate_suggestion_count": duplicate_suggestion_count,
            "consolidation_by_check": category_accuracy,
            "input_tokens": input_tokens, "output_tokens": output_tokens,
        },
        "consolidation_cases": consolidation_results,
        "owner_outcomes": "Approval/dismissal persistence and authorization remain covered by transient Postgres/API tests.",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Confirm real provider calls and charges")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required; this evaluation calls Gemini")
    report = run(args.output)
    print(json.dumps({"output": str(args.output), "metrics": report["metrics"]}, indent=2))


if __name__ == "__main__":
    main()
