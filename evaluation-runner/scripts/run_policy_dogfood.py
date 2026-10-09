#!/usr/bin/env python3
"""Run paired policy-evaluation cases against a live theLake evaluation runner."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "evaluation-runner" / "dogfood" / "connected_itinerary.json"


def post_evaluation(endpoint: str, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=150) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        # The runner intentionally returns a generic provider error. Avoid
        # echoing request headers or environment variables in diagnostics.
        raise RuntimeError(f"evaluation runner returned HTTP {error.code}") from None
    except urllib.error.URLError as error:
        raise RuntimeError(f"could not reach evaluation runner: {error.reason}") from None


def validate_response(case: dict[str, Any], response: dict[str, Any]) -> list[str]:
    errors = []
    expected = case["expected_status"]
    if response.get("status") != expected:
        errors.append(f"expected status {expected}, got {response.get('status')}")
    expected_trace_id = case["evidence"]["trace_id"]
    if response.get("trace_id") != expected_trace_id:
        errors.append(
            f"expected trace_id {expected_trace_id}, got {response.get('trace_id')}"
        )
    score = response.get("score")
    if expected in {"pass", "fail"}:
        if not isinstance(score, (int, float)) or isinstance(score, bool):
            errors.append("pass/fail verdict must include a numeric score")
        elif not math.isfinite(score) or not 0 <= score <= 1:
            errors.append("score must be finite and between 0 and 1")
        evidence = response.get("evidence")
        if not isinstance(evidence, list) or not evidence or any(
            not isinstance(reference, dict)
            or not isinstance(reference.get("span_id"), str)
            or not reference["span_id"].strip()
            for reference in evidence
        ):
            errors.append("pass/fail verdict must include valid evidence span references")
    elif score is not None:
        errors.append("insufficient evidence must not include a judge score")
    return errors


def run(endpoint: str, token: str, output_path: Path | None = None) -> list[dict[str, Any]]:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    evaluator = fixture["evaluator"]
    results = []
    failures = []

    for case in fixture["cases"]:
        run_id = f"dogfood:{case['case_id']}:{uuid.uuid4().hex[:12]}"
        payload = {
            "run_id": run_id,
            "evaluator": evaluator,
            "evidence": case["evidence"],
        }
        started = time.monotonic()
        response = post_evaluation(endpoint, token, payload)
        elapsed_ms = round((time.monotonic() - started) * 1000)
        expected = case["expected_status"]
        actual = response.get("status")
        validation_errors = validate_response(case, response)
        result = {
            "case_id": case["case_id"],
            "expected_status": expected,
            "actual_status": actual,
            "score": response.get("score"),
            "matched": not validation_errors,
            "validation_errors": validation_errors,
            "elapsed_ms": elapsed_ms,
            "trace_id": response.get("trace_id"),
            "evidence": response.get("evidence", []),
            "rationale": response.get("rationale", ""),
            "limitations": response.get("limitations", []),
        }
        results.append(result)
        if validation_errors:
            failures.append(case["case_id"])
        print(json.dumps(result, ensure_ascii=False))

    report = {
        "suite": "connected-itinerary-policy-dogfood",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "evaluator_id": evaluator["evaluator_id"],
        "evaluator_version": evaluator["version"],
        "case_count": len(results),
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "results": results,
    }
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        output_path.chmod(0o600)
        print(f"Report written to {output_path}", file=sys.stderr)
    if failures:
        raise RuntimeError(f"dogfood verdict mismatch: {', '.join(failures)}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.getenv("THELAKE_EVALUATION_RUNNER_URL"),
        help="Evaluation runner URL (defaults to THELAKE_EVALUATION_RUNNER_URL).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON report path. The report includes verdicts and evidence references, not trace payloads or credentials.",
    )
    args = parser.parse_args()
    token = os.getenv("THELAKE_EVALUATION_RUNNER_TOKEN") or os.getenv(
        "EVALUATION_RUNNER_TOKEN"
    )
    if not args.url:
        parser.error("set THELAKE_EVALUATION_RUNNER_URL or pass --url")
    if not token:
        parser.error("set THELAKE_EVALUATION_RUNNER_TOKEN")
    try:
        results = run(args.url, token, args.output)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        parser.exit(1, f"dogfood evaluation failed: {error}\n")
    print(
        f"Dogfood suite passed: {len(results)} cases against the live evaluation runner.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
