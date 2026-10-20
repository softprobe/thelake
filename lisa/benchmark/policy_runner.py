"""Quality harness: score labeled dataset pairs through Lisa's production path.

Offline harness only — see ``benchmark`` package docs. Do not relocate into
``lisa.evaluation``.

Goal
----
Measure triage/judge quality on adjudicated ``(session_id, policy_id)`` pairs.
Annotations are scoring truth only. Candidate policies always come from Lisa's
PostgreSQL catalog (seeded from the catalog manifest), never from the label set.

Inputs
------
CLI / ``main``:

- ``--manifest``: dataset manifest whose batches, trajectories, and adjudicated
  annotations define *what to score* (pair_data).
- ``--catalog-manifest``: dataset manifest whose policies define the *full*
  approved catalog written into Postgres (catalog). May be larger than the
  labeled subset in ``--manifest``.
- ``--output``: path for the JSON report written at the end.
- ``--dogfood-env-file``: local Compose env with DB/API ports, workspace ID,
  Lisa DB passwords, and TheLake token (no credentials printed).
- ``--data-dir``: directory for Lisa home / recorded LLM calls (defaults beside
  ``--output``).
- ``--live``: required flag; authorizes paid provider calls.

``evaluate(pair_data, catalog, output_path)`` after env is configured:

- ``pair_data``: loaded ``--manifest`` (sessions + labels).
- ``catalog``: loaded ``--catalog-manifest`` (approved policies to seed).
- ``output_path``: same as ``--output``.
- Process env: ``LISA_*`` / ``SOFTPROBE_PUBLIC_KEY`` pointing at localhost only.

Outputs
-------

- JSON report at ``output_path`` / ``--output``, including:
  - ``rows``: per labeled pair — ``session_id``, ``policy_id``, ``expected``,
    triage ``decision``, ``escalated``, judge ``verdict``, optional ``gate_status``.
  - ``sessions``: per fixture session status and candidate counts.
  - ``metrics`` / ``per_policy_support``: accuracy-style summaries over non-gated rows.
  - digests, agent name, workspace id, approved policy id map.
- Stdout: ``metrics`` JSON only (CLI).
- During the CLI run: TheLake sessions under fixture ``session_id``s and Lisa
  Postgres policies in the isolated dogfood stack. The CLI removes these with
  the stack when the run exits, including on failure. LLM call traces under
  ``--data-dir`` are retained; a report is retained when evaluation produces one.

Lifecycle (local disposable workspace only)
-------------------------------------------
1. Setup (``main`` → ``conftest.configure_lisa_dogfood`` → ``evaluate``):
   - Point Lisa at the local dogfood DB/API; record LLM calls under ``--data-dir``.
   - Refuse remote hosts; require ``--live`` and ``LISA_LLM_TRACE_ENABLED``.
   - Mint a unique agent name; refuse if any fixture ``session_id`` already exists.
   - Seed the full approved policy catalog for that agent through the
     benchmark-only seed helper in this run-owned database.

2. Execute (``evaluate``):
   - For each complete fixture session: ingest OTLP under the dataset ``session_id``,
     wait until TheLake can serve it, and verify ``SessionService`` reconstruction.
   - Incomplete/unordered fixtures are capture-gated (listed in report, no providers).
   - After every session reconstructs, call ``EvaluationService.evaluate_session`` once
     per session; join decisions/verdicts to adjudicated labels; write the JSON report.

3. Cleanup:
   - When the CLI exits, ``conftest.dispose_lisa_dogfood`` removes the run-owned
     Compose project and its volumes, then deletes that run's DuckLake data
     directory, including ingested sessions and Lisa's seeded policies. It refuses
     to clean other resources.
   - The report and ``--data-dir`` LLM traces remain on disk; ``evaluate`` closes
     the triage HTTP client. Programmatic callers own their disposable resources.
"""

from __future__ import annotations

from benchmark.policy_seed import seed_policy_document

import argparse
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from conftest import (
    configure_lisa_dogfood,
    dispose_lisa_dogfood,
    lisa_dogfood_target,
)
from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.models.policies import PolicyRecord
from lisa.security import redact_evidence
from datasets.policy_dataset import Dataset, load_dataset
from benchmark.policy_ingest import build_otlp_trace_export


def _render_policy_markdown(records: list[PolicyRecord]) -> str:
    return "\n\n".join(
        f"## {record.id} | {record.name}\napproved: {'yes' if record.approved else 'no'}\n\n{record.criteria}"
        for record in records
    )


def _add_catalog_to_policy_management(
    policy_service,
    catalog: Dataset,
    target_agent_name: str,
) -> dict[str, str]:
    """Seed a disposable benchmark database from a contiguous readable-ID catalog."""
    catalog_names = [item.policy.name.casefold() for item in catalog.policies.values()]
    if len(catalog_names) != len(set(catalog_names)):
        raise ValueError("Lisa's policy catalog must have unique policy names")
    if any(not item.policy.approved for item in catalog.policies.values()):
        raise ValueError("Every policy in the production evaluation catalog must be approved")
    expected_catalog_ids = [f"P1.{index}" for index in range(1, len(catalog.policies) + 1)]
    actual_catalog_ids = sorted(
        (item.policy_id for item in catalog.policies.values()),
        key=lambda identifier: int(identifier.split(".")[1]),
    )
    if actual_catalog_ids != expected_catalog_ids:
        raise ValueError("Benchmark catalog policy IDs must be contiguous P1.1 through P1.n")

    for policy in sorted(
        catalog.policies.values(), key=lambda item: int(item.policy_id.split(".")[1])
    ):
        current = policy_service.read_policy_records("agent", target_agent_name)
        expected = _render_policy_markdown(current)
        rules = {record.name: record for record in current}
        rules[policy.policy.name] = PolicyRecord(
            id=policy.policy_id,
            scope="agent",
            target_agent_name=target_agent_name,
            name=policy.policy.name,
            criteria=policy.policy.criteria,
            approved=policy.policy.approved,
        )
        content = _render_policy_markdown(list(rules.values()))
        seed_policy_document(
            policy_service, content, expected, scope="agent", agent_name=target_agent_name
        )

    loaded = policy_service.approved_rules(target_agent_name)
    expected_ids = {item.policy.name: item.policy_id for item in catalog.policies.values()}
    loaded_ids = {item.name: item.id for item in loaded}
    if loaded_ids != expected_ids or len(loaded) != len(expected_ids):
        raise RuntimeError(
            "Lisa's approved PostgreSQL policy IDs do not match the dataset catalog"
        )
    return loaded_ids


def _post_dataset_trace(api_url: str, bearer_token: str, payload: dict) -> dict:
    request = Request(
        f"{api_url.rstrip('/')}/v1/traces",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        raise RuntimeError(f"TheLake trace ingest failed with HTTP {error.code}") from None
    except URLError as error:
        raise RuntimeError(f"TheLake trace ingest failed: {error.reason}") from None
    if body.get("success") is not True:
        raise RuntimeError("TheLake did not confirm dataset trace ingestion")
    return body


def _wait_for_session(services, session_id: str, expected_spans: int) -> dict:
    deadline = time.monotonic() + 30
    while True:
        try:
            detail = services.sessions.detail(session_id)
        except RuntimeError as error:
            # Session summaries are reduced asynchronously after trace ingest.
            # Retry only the API's not-found response while that index catches up.
            if "(404)" not in str(error):
                raise
        else:
            if len(detail.get("spans", [])) >= expected_spans:
                return detail
        if time.monotonic() >= deadline:
            raise TimeoutError(f"TheLake session {session_id} did not become queryable")
        time.sleep(0.5)


def _ensure_sessions_absent(services, session_ids: list[str]) -> None:
    """Do not append dataset spans to sessions from a prior evaluation run."""
    for session_id in session_ids:
        try:
            services.sessions.detail(session_id)
        except RuntimeError as error:
            if "(404)" in str(error):
                continue
            raise
        raise RuntimeError(
            f"Dataset session {session_id} already exists; use an empty disposable workspace"
        )


def _trajectory_semantics(events: list[dict]) -> list[dict]:
    fields = ("kind", "name", "call_id", "content", "payload", "status")
    return [{key: event[key] for key in fields if key in event} for event in events]


def _recorded_triage_question_count(recording_root: Path, session_id: str) -> int:
    matches = []
    for path in recording_root.glob("*.jsonl"):
        try:
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        except (OSError, json.JSONDecodeError):
            continue
        if not rows:
            continue
        request = rows[0]
        if (
            request.get("type") == "request"
            and request.get("surface") == "policy_triage"
            and request.get("correlation", {}).get("session_id") == session_id
        ):
            matches.append(request)
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one recorded Decisions request for {session_id}; found {len(matches)}"
        )
    questions = matches[0].get("request", {}).get("questions")
    if not isinstance(questions, list):
        raise TypeError(f"Recorded Decisions request for {session_id} has no question list")
    return len(questions)


def _summary(rows: list[dict]) -> dict:
    resolved = [
        row for row in rows if row["expected"] in {"violation", "compliant", "not_applicable"}
    ]
    tp = sum(row["expected"] == "violation" and row["escalated"] for row in resolved)
    fn = sum(row["expected"] == "violation" and not row["escalated"] for row in resolved)
    fp = sum(row["expected"] != "violation" and row["escalated"] for row in resolved)
    tn = sum(row["expected"] != "violation" and not row["escalated"] for row in resolved)
    failures = sum(row["expected"] == "violation" and row["verdict"] == "fail" for row in rows)
    missed = sum(row["expected"] == "violation" and row["verdict"] != "fail" for row in rows)
    detected = sum(row["expected"] == "violation" and row["verdict"] == "fail" for row in resolved)
    false_detection = sum(
        row["expected"] != "violation" and row["verdict"] == "fail" for row in resolved
    )
    no_detection = sum(
        row["expected"] != "violation" and row["verdict"] != "fail" for row in resolved
    )
    insufficient = [row for row in rows if row["expected"] == "insufficient_evidence"]
    insufficient_escalated = sum(row["escalated"] for row in insufficient)
    return {
        "adjudicated_pair_count": len(rows),
        "binary_resolved_pair_count": len(resolved),
        "triage_confusion": {"tp": tp, "fn": fn, "fp": fp, "tn": tn},
        "violation_recall": tp / (tp + fn) if tp + fn else None,
        "triage_precision": tp / (tp + fp) if tp + fp else None,
        "triage_accuracy": (tp + tn) / len(resolved) if resolved else None,
        "end_to_end_violation_recall": failures / (failures + missed)
        if failures + missed
        else None,
        "end_to_end_detection_confusion": {
            "tp": detected,
            "fn": len([row for row in resolved if row["expected"] == "violation"]) - detected,
            "fp": false_detection,
            "tn": no_detection,
        },
        "end_to_end_precision": detected / (detected + false_detection)
        if detected + false_detection
        else None,
        "end_to_end_accuracy": (detected + no_detection) / len(resolved) if resolved else None,
        "insufficient_evidence_count": len(insufficient),
        "uncertain_escalation_recall": insufficient_escalated / len(insufficient)
        if insufficient
        else None,
    }


def _per_policy_support(rows: list[dict]) -> dict[str, dict]:
    support = {}
    for row in rows:
        result = support.setdefault(
            row["policy_id"],
            {
                "pair_count": 0,
                "outcomes": {},
                "violation_tp": 0,
                "violation_fn": 0,
                "labeled_negative_escalations": 0,
            },
        )
        result["pair_count"] += 1
        outcome = row["expected"]
        result["outcomes"][outcome] = result["outcomes"].get(outcome, 0) + 1
        if outcome == "violation":
            result["violation_tp" if row["escalated"] else "violation_fn"] += 1
        elif outcome in {"compliant", "not_applicable"} and row["escalated"]:
            result["labeled_negative_escalations"] += 1
    return support


def _validate_evaluation_result(result: dict, session_id: str) -> None:
    evaluations = result.get("evaluations", [])
    failures = [
        item
        for item in evaluations
        if item.get("status") in {"error", "already_running", "policy_unavailable"}
    ]
    if result.get("status") == "evaluation_error" or failures:
        statuses = sorted({item.get("status", "unknown") for item in failures})
        raise RuntimeError(
            f"Lisa production evaluation failed for {session_id} ({', '.join(statuses) or result.get('status')})"
        )


def evaluate(pair_data: Dataset, catalog: Dataset, output_path: Path) -> dict:
    """Run one harness pass: setup → ingest/verify → evaluate → report.

    Inputs:
        pair_data: Scoring dataset — trajectories keyed by ``session_id``, sparse
            adjudicated annotations, and manifest batches listing candidate
            ``policy_ids`` per session. Labels are compared after evaluation;
            they are never passed into the triager/judge.
        catalog: Policy catalog dataset — every approved ``PolicySpec`` seeded
            into Postgres for this run's agent. Must be the full candidate set
            Lisa should triage (not only labeled pairs).
        output_path: File to write the JSON report.

    Returns:
        The same report dict written to ``output_path``. Key fields:
        ``rows`` (pair-level expected vs decision/verdict), ``sessions``,
        ``metrics``, ``per_policy_support``, ``approved_policy_ids_by_name``.
    """
    # --- Setup: local-only services, unique agent, empty of fixture sessions ---
    context = WorkspaceContext.from_environment()
    if not context.database_url:
        raise RuntimeError("Production evaluation requires Lisa's PostgreSQL policy database")
    if not context.api_url or not context.api_token:
        raise RuntimeError("Production evaluation requires the configured TheLake session API")
    database_host = urlsplit(context.database_url).hostname
    api_host = urlsplit(context.api_url).hostname
    if database_host not in {"localhost", "127.0.0.1", "::1"} or api_host not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        raise RuntimeError("Dataset evaluation is restricted to a disposable local Lisa workspace")
    os.environ["LISA_DATA_DIR"] = str(context.data_dir.resolve())
    if os.getenv("LISA_LLM_TRACE_ENABLED", "").lower() not in {"1", "true", "yes"}:
        raise RuntimeError("Set LISA_LLM_TRACE_ENABLED=1 to record production provider requests")

    services = build_services(context)
    try:
        return _evaluate_with_services(pair_data, catalog, output_path, context, services)
    finally:
        client = services.evaluations.policy_triage.client
        if client is not None:
            client.close()


def _evaluate_with_services(
    pair_data: Dataset,
    catalog: Dataset,
    output_path: Path,
    context: WorkspaceContext,
    services,
) -> dict:
    run_id = uuid4().hex[:12]
    agent_name = f"lisa-policy-eval-{run_id}"
    ingest_session_ids = [
        batch.session_id
        for batch in pair_data.manifest.batches
        if pair_data.trajectories[batch.session_id].trajectory.complete
        and pair_data.trajectories[batch.session_id].trajectory.tool_order_certain
        and pair_data.trajectories[batch.session_id].trajectory.message_order_certain
    ]
    # Fixture session_ids are ingested as-is; refuse if they already exist so we
    # never append spans onto prior harness evidence.
    _ensure_sessions_absent(services, ingest_session_ids)
    policy_ids = _add_catalog_to_policy_management(services.policy_service, catalog, agent_name)

    # --- Execute phase A: ingest each complete session and verify reconstruction ---
    ingest_token = os.getenv("SOFTPROBE_PUBLIC_KEY", "dogfood-local")
    rows = []
    sessions = []
    prepared = []
    for batch in pair_data.manifest.batches:
        record = pair_data.trajectories[batch.session_id]
        trajectory = record.trajectory.model_dump(mode="json", exclude_unset=True)
        if not (
            trajectory["complete"]
            and trajectory["tool_order_certain"]
            and trajectory["message_order_certain"]
        ):
            # Capture-gated: record labels for coverage, skip providers for this session.
            sessions.append(
                {
                    "session_id": batch.session_id,
                    "status": "capture_gated",
                    "candidate_count": len(policy_ids),
                    "policy_ids": policy_ids,
                }
            )
            for pair in pair_data.pairs(batch, adjudicated_only=True):
                rows.append(
                    {
                        "session_id": batch.session_id,
                        "policy_id": pair.policy.policy_id,
                        "expected": pair.annotation.outcome,
                        "decision": None,
                        "escalated": None,
                        "verdict": None,
                        "gate_status": "capture_gated",
                    }
                )
            continue

        session_id = record.session_id
        trace_id = trajectory["trace_id"]
        scoped = trajectory
        payload = build_otlp_trace_export(
            scoped,
            session_id=session_id,
            agent_name=agent_name,
            trace_id=trace_id,
        )
        span_count = len(payload["resourceSpans"][0]["scopeSpans"][0]["spans"])
        _post_dataset_trace(context.api_url, ingest_token, payload)
        detail = _wait_for_session(services, session_id, span_count)
        reconstructed = services.sessions.trajectory(detail)
        if not (
            reconstructed["complete"]
            and reconstructed["tool_order_certain"]
            and reconstructed["message_order_certain"]
        ):
            raise RuntimeError(
                f"SessionService reconstructed an incomplete or unordered trajectory for {batch.session_id}: "
                f"{reconstructed['limitations']}"
            )
        if _trajectory_semantics(reconstructed["events"]) != _trajectory_semantics(
            redact_evidence(scoped["events"])
        ):
            raise RuntimeError(
                f"TheLake changed trajectory event semantics for {batch.session_id}"
            )

        prepared.append((batch, record, session_id, trace_id))

    # --- Execute phase B: one production evaluate_session per verified session ---
    # All sessions are verified first so a late fidelity failure does not burn
    # paid Decisions calls on a partial batch.
    for batch, record, session_id, trace_id in prepared:
        result = services.evaluations.evaluate_session(agent_name, session_id)

        _validate_evaluation_result(result, session_id)
        decisions = {item["name"]: item for item in result.get("policy_decisions", [])}
        if len(decisions) != len(policy_ids) or set(decisions) != set(policy_ids):
            raise RuntimeError(
                f"Lisa evaluated {len(decisions)} candidate policies; expected {len(policy_ids)}"
            )
        if {name: item.get("id") for name, item in decisions.items()} != policy_ids:
            raise RuntimeError(
                "Lisa's production triage decisions do not match the PostgreSQL policy IDs"
            )
        request_question_count = _recorded_triage_question_count(
            context.home() / "llm-calls", session_id
        )
        if request_question_count != len(policy_ids):
            raise RuntimeError(
                f"Recorded Decisions request included {request_question_count} policies; "
                f"PostgreSQL contains {len(policy_ids)} approved candidates"
            )
        verdicts = {item["evaluation_name"]: item for item in result.get("evaluations", [])}
        sessions.append(
            {
                "session_id": batch.session_id,
                "trace_id": trace_id,
                "status": result.get("status"),
                "candidate_count": len(decisions),
                "recorded_decisions_question_count": request_question_count,
                "candidate_policy_ids": policy_ids,
                "escalated_count": len(result.get("selected_policies", [])),
                "judge_count": len(verdicts),
            }
        )
        for pair in pair_data.pairs(batch, adjudicated_only=True):
            decision = decisions[pair.policy.policy.name]["decision"]
            verdict = verdicts.get(pair.policy.policy.name, {}).get("status")
            rows.append(
                {
                    "session_id": batch.session_id,
                    "policy_id": pair.policy.policy_id,
                    "expected": pair.annotation.outcome,
                    "decision": decision,
                    "escalated": decision != "no_violation_found",
                    "verdict": verdict,
                    "gate_status": None,
                }
            )

    # --- Finish: write the report before the CLI removes its disposable stack ---
    report = {
        "run_id": run_id,
        "target_agent_name": agent_name,
        "workspace_id": context.workspace_id(),
        "dataset_manifest": pair_data.manifest.name,
        "dataset_digest": pair_data.digest,
        "catalog_manifest": catalog.manifest.name,
        "catalog_digest": catalog.digest,
        "approved_candidate_count": len(policy_ids),
        "approved_policy_ids_by_name": policy_ids,
        "triage_model": services.evaluations.policy_triage.model,
        "sessions": sessions,
        "rows": rows,
        "metrics": _summary([row for row in rows if row["gate_status"] is None]),
        "per_policy_support": _per_policy_support(
            [row for row in rows if row["gate_status"] is None]
        ),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    """CLI entry: wire dogfood env, load manifests, run ``evaluate``, print metrics.

    Inputs: ``--manifest`` (score), ``--catalog-manifest`` (seed policies),
    ``--output`` (report path), ``--dogfood-env-file``, ``--data-dir``, ``--live``.
    Outputs: report file at ``--output``; ``metrics`` JSON on stdout.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Input: scoring dataset manifest (sessions + adjudicated pair labels)",
    )
    parser.add_argument(
        "--catalog-manifest",
        type=Path,
        default=Path("lisa/datasets/policy_evaluation/manifests/evaluation.json"),
        help="Input: full approved-policy catalog to seed; never infer from labeled pairs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output: JSON report path (rows, sessions, metrics)",
    )
    parser.add_argument(
        "--dogfood-env-file",
        type=Path,
        default=Path("warehouse/dogfood/compose.env"),
        help="Input: local DB/API connection metadata created by dogfood",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Output: Lisa data dir for recorded LLM calls (default beside --output)",
    )
    parser.add_argument("--live", action="store_true", help="Allow paid provider calls")
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required to authorize provider calls")
    # Setup: bind process env to the disposable dogfood stack + isolated data dir.
    data_dir = args.data_dir or args.output.parent / f".{args.output.stem}-lisa-data"
    repository_root = Path(__file__).resolve().parents[2]
    cleanup_target = lisa_dogfood_target(args.dogfood_env_file, repository_root)
    for retained_path in (args.output.resolve(), data_dir.resolve()):
        if retained_path == cleanup_target.lake_data or cleanup_target.lake_data in retained_path.parents:
            parser.error("--output and --data-dir must be outside the disposable dogfood data path")
    try:
        configure_lisa_dogfood(args.dogfood_env_file, data_dir.resolve(), repository_root)
        report = evaluate(
            load_dataset(args.manifest), load_dataset(args.catalog_manifest), args.output
        )
    finally:
        dispose_lisa_dogfood(
            cleanup_target,
            repository_root,
            report_path=args.output,
            data_dir=data_dir,
        )
    print(json.dumps(report["metrics"], indent=2))


if __name__ == "__main__":
    main()
