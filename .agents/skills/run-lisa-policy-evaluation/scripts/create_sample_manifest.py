"""Create an isolated, stratified manifest for a small Lisa policy evaluation."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Offline harness lives outside the installable runtime package.
_REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO / "lisa" / "benchmarks"))

from lisa_benchmarks.dataset import DEFAULT_MANIFEST, load_dataset

OUTCOMES = ("violation", "compliant", "not_applicable", "insufficient_evidence")


def choose_pairs(data, *, count: int, split: str, seed: int):
    candidates = [
        pair
        for batch in data.batches(split=split)
        for pair in data.pairs(batch, adjudicated_only=True)
    ]
    by_outcome: dict[str, list] = defaultdict(list)
    for pair in candidates:
        by_outcome[pair.annotation.outcome].append(pair)

    base, remainder = divmod(count, len(OUTCOMES))
    quotas = {
        outcome: base + (index < remainder) for index, outcome in enumerate(OUTCOMES)
    }
    shortfalls = {
        outcome: (quota, len(by_outcome[outcome]))
        for outcome, quota in quotas.items()
        if len(by_outcome[outcome]) < quota
    }
    if shortfalls:
        raise ValueError(f"Split {split!r} cannot satisfy outcome quotas: {shortfalls}")

    rng = random.Random(seed)
    for group in by_outcome.values():
        rng.shuffle(group)

    # Fill rare classes first. Within each class, favor an unused trajectory and
    # the least represented source while preserving the requested label quotas.
    selected = []
    used_trajectories: set[str] = set()
    source_counts: Counter[str] = Counter()
    outcome_counts: Counter[str] = Counter()
    for outcome in sorted(
        OUTCOMES, key=lambda item: (len(by_outcome[item]), OUTCOMES.index(item))
    ):
        for _ in range(quotas[outcome]):
            available = [pair for pair in by_outcome[outcome] if pair not in selected]
            fresh = [
                pair
                for pair in available
                if pair.trajectory.session_id not in used_trajectories
            ]
            pool = fresh or available
            if not pool:
                raise ValueError(f"Unable to fill sample quota for {outcome}")
            pool.sort(
                key=lambda pair: (
                    source_counts[pair.trajectory.provenance["source"]],
                    outcome_counts[(outcome, pair.trajectory.provenance["source"])],
                    rng.random(),
                )
            )
            pair = pool[0]
            selected.append(pair)
            used_trajectories.add(pair.trajectory.session_id)
            source_counts[pair.trajectory.provenance["source"]] += 1
            outcome_counts[(outcome, pair.trajectory.provenance["source"])] += 1

    # Keep a stable readable order in the report and exact 1-pair annotations
    # under each selected trajectory batch.
    selected.sort(
        key=lambda pair: (
            pair.trajectory.session_id,
            pair.annotation.outcome,
            pair.policy.policy_id,
        )
    )
    return selected


def write_manifest(data, pairs, *, output_root: Path, split: str, seed: int):
    output_root.mkdir(parents=True, exist_ok=True)
    if any(output_root.iterdir()):
        raise ValueError(f"Output directory must be empty: {output_root}")

    source_root = DEFAULT_MANIFEST.parent.parent
    files = {}
    for name in ("trajectories", "policies"):
        shutil.copyfile(source_root / f"{name}.jsonl", output_root / f"{name}.jsonl")
        files[name] = hashlib.sha256((output_root / f"{name}.jsonl").read_bytes()).hexdigest()
    annotations = [pair.annotation for pair in pairs]
    annotation_path = output_root / "annotations.jsonl"
    annotation_path.write_text(
        "".join(annotation.model_dump_json() + "\n" for annotation in annotations)
    )
    files["annotations"] = hashlib.sha256(annotation_path.read_bytes()).hexdigest()

    by_trajectory: dict[str, list] = defaultdict(list)
    for pair in pairs:
        by_trajectory[pair.trajectory.session_id].append(pair)

    source_batches = {
        batch.session_id: batch for batch in data.batches(split=split)
    }
    batches = []
    for session_id, selected in sorted(by_trajectory.items()):
        batches.append(
            {
                "session_id": session_id,
                "policy_ids": [pair.policy.policy_id for pair in selected],
                "split": source_batches[session_id].split,
            }
        )

    manifest = {
        "name": f"{data.manifest.name}-sample-{len(pairs)}-seed-{seed}",
        "files": files,
        "batches": batches,
    }
    manifest_dir = output_root / "manifests"
    manifest_dir.mkdir()
    manifest_path = manifest_dir / "sample.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    # Validate the reduced catalog with the production dataset loader before use.
    sampled = load_dataset(manifest_path)
    loaded_pairs = sampled.pairs(adjudicated_only=True)
    if len(loaded_pairs) != len(pairs):
        raise RuntimeError("Sample manifest did not preserve the selected pair count")
    if Counter(pair.annotation.outcome for pair in loaded_pairs) != Counter(
        pair.annotation.outcome for pair in pairs
    ):
        raise RuntimeError("Sample manifest changed the selected outcome distribution")
    return manifest_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=16, choices=range(10, 21))
    parser.add_argument("--split", choices=("development", "benchmark"), default="benchmark")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()

    data = load_dataset(DEFAULT_MANIFEST)
    pairs = choose_pairs(data, count=args.count, split=args.split, seed=args.seed)
    manifest_path = write_manifest(
        data, pairs, output_root=args.output_root, split=args.split, seed=args.seed
    )

    summary = {
        "manifest": str(manifest_path),
        "seed": args.seed,
        "split": args.split,
        "pair_count": len(pairs),
        "outcomes": dict(Counter(pair.annotation.outcome for pair in pairs)),
        "sources": dict(
            Counter(pair.trajectory.provenance["source"] for pair in pairs)
        ),
        "unique_sessions": len({pair.trajectory.session_id for pair in pairs}),
        "pairs": [
            {
                "session_id": pair.trajectory.session_id,
                "trajectory_name": pair.trajectory.name,
                "policy_id": pair.policy.policy_id,
                "policy_name": pair.policy.policy.name,
                "outcome": pair.annotation.outcome,
                "source": pair.trajectory.provenance["source"],
            }
            for pair in pairs
        ],
    }
    summary_text = json.dumps(summary, indent=2) + "\n"
    (args.output_root / "sample-summary.json").write_text(summary_text)
    print(summary_text, end="")


if __name__ == "__main__":
    main()
