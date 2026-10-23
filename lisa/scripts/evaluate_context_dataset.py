"""Run Lisa's separate live context-isolation and checkpoint benchmark."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.activity_runner import run_benchmark

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "lisa" / "datasets" / "conversation_context" / "manifests" / "benchmark.json"
ARTIFACTS = ROOT / "target" / "test-artifacts" / "lisa-context-benchmark"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--split", choices=("benchmark", "all"), default="benchmark"
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--live", action="store_true", help="Confirm real model calls and provider charges"
    )
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required because the context benchmark invokes Lisa's model")
    if os.getenv("LISA_BASE_URL") or os.getenv("LISA_API_KEY"):
        parser.error("The live context benchmark requires the direct Gemini production path")
    model = os.getenv("LISA_MODEL", "gemini/gemini-3.8-flash")
    if not model.startswith("gemini/"):
        parser.error("The live context benchmark requires a Gemini model")
    if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
        parser.error("Set GOOGLE_API_KEY or GEMINI_API_KEY for the live Gemini benchmark")

    # Keep these test-only limits local to this process. The real model window
    # and deployment settings are unchanged after the benchmark exits.
    os.environ["LISA_MODEL"] = model
    os.environ["LISA_CONTEXT_WINDOW_TOKENS"] = "512"
    os.environ["LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO"] = "0.5"
    os.environ["LISA_CONTEXT_RETAINED_TAIL_TOKENS"] = "128"

    run_id = uuid4().hex[:12]
    output = (args.output or ARTIFACTS / run_id / "report.json").resolve()
    if not output.is_relative_to(ARTIFACTS.resolve()):
        parser.error(f"Benchmark reports must be saved under {ARTIFACTS}")
    if output.exists() or output.parent.exists() and any(output.parent.iterdir()):
        parser.error(f"Refusing to overwrite an existing benchmark directory: {output.parent}")

    report = run_benchmark(
        MANIFEST,
        args.split,
        output,
        process_context_checkpoints=True,
    )
    print(
        json.dumps(
            {
                "report": str(output),
                "status": report["status"],
                "passed_cases": report["passed_cases"],
                "case_count": report["case_count"],
            }
        )
    )
    if report["status"] == "failed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
