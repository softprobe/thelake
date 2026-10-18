"""Evaluate labeled trajectories through Lisa's PostgreSQL-backed service path."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import replace
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.evaluation.dataset import Dataset, load_dataset
from lisa.evaluation.policy_dataset_ingest import build_otlp_trace_export
from lisa.models.policies import PolicyRecord
from lisa.security import redact_evidence
from lisa.services.policies import PolicyService


def _render_policy_markdown(records: list[PolicyRecord]) -> str:
    return "\n\n".join(
        f"## {record.name}\napproved: {'yes' if record.approved else 'no'}\n\n{record.criteria}"
        for record in records
    )


def _add_catalog_to_policy_management(
    context: WorkspaceContext,
    policy_service: PolicyService,
    catalog: Dataset,
    target_agent_name: str,
) -> dict[str, str]:
    """Approve catalog rules via Lisa's owner-gated PolicyService API."""
    catalog_names = [item.policy.name.casefold() for item in catalog.policies.values()]
    if len(catalog_names) != len(set(catalog_names)):
        raise ValueError("Lisa's policy catalog must have unique policy names")
    if any(not item.policy.approved for item in catalog.policies.values()):
        raise ValueError("Every policy in the production evaluation catalog must be approved")

    for policy in sorted(catalog.policies.values(), key=lambda item: item.ref):
        current = policy_service.read_policy_records("agent", target_agent_name)
        expected = _render_policy_markdown(current)
        rules = {record.name: record for record in current}
        rules[policy.policy.name] = PolicyRecord(
            id="",
            scope="agent",
            target_agent_name=target_agent_name,
            name=policy.policy.name,
            criteria=policy.policy.criteria,
            approved=policy.policy.approved,
        )
        content = _render_policy_markdown(list(rules.values()))
        managed = PolicyService(
            replace(
                context,
                turn_source="web",
                confirmed_policy_title=policy.policy.name,
            ),
            policy_service.repository,
        )
        managed.update_policy(content, expected, scope="agent", agent_name=target_agent_name)

    loaded = policy_service.approved_rules(target_agent_name)
    expected_names = {item.policy.name for item in catalog.policies.values()}
    loaded_names = {item.name for item in loaded}
    if loaded_names != expected_names or len(loaded) != len(expected_names):
        raise RuntimeError(
            "Lisa's approved PostgreSQL policy set does not match the dataset catalog"
        )
    return {rule.name: rule.id for rule in loaded}


def _run_scoped_ids(run_id: str, trajectory_ref: str, trajectory: dict) -> tuple[str, dict]:
    trace_id = hashlib.sha256(f"{run_id}:{trajectory_ref}".encode()).hexdigest()[:32]
    events = []
    for event in trajectory["events"]:
        copy = dict(event)
        source_id = str(event["span_id"])
        copy["span_id"] = hashlib.sha256(
            f"{run_id}:{trajectory_ref}:{event['sequence']}:{source_id}".encode()
        ).hexdigest()[:16]
        events.append(copy)
    return trace_id, {**trajectory, "trace_id": trace_id, "events": events}


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
            row["policy_ref"],
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


def _read_environment_file(path: Path) -> dict[str, str]:
    values = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _configure_quickstart_environment(path: Path, data_dir: Path) -> None:
    """Read local quickstart connection settings without printing credentials."""
    values = _read_environment_file(path)
    workspace_id = values.get("THELAKE_DEFAULT_WORKSPACE_ID")
    database_port = values.get("THELAKE_QUICKSTART_DB_PORT")
    api_port = values.get("THELAKE_QUICKSTART_PORT")
    writer_password = values.get("LISA_DATABASE_WRITER_PASSWORD")
    reader_password = values.get("LISA_DATABASE_READER_PASSWORD")
    service_token = values.get("LISA_THELAKE_TOKEN")
    if not all(
        (workspace_id, database_port, api_port, writer_password, reader_password, service_token)
    ):
        raise RuntimeError("Quickstart connection metadata is incomplete; start an isolated stack")
    os.environ["LISA_WORKSPACE_ID"] = workspace_id
    safe_writer_password = quote(writer_password, safe="")
    os.environ["LISA_DATABASE_URL"] = (
        f"postgresql://lisa_writer:{safe_writer_password}@127.0.0.1:{database_port}/lisa"
    )
    safe_reader_password = quote(reader_password, safe="")
    os.environ["LISA_DATABASE_READ_URL"] = (
        f"postgresql://lisa_reader:{safe_reader_password}@127.0.0.1:{database_port}/lisa"
    )
    os.environ["LISA_THELAKE_API_URL"] = f"http://127.0.0.1:{api_port}"
    os.environ["LISA_THELAKE_TOKEN"] = service_token
    # This runner must never put policy call recordings in a user's Lisa home.
    os.environ["LISA_DATA_DIR"] = str(data_dir.resolve())
    os.environ["LISA_LLM_TRACE_ENABLED"] = "1"
    os.environ["SOFTPROBE_PUBLIC_KEY"] = "quickstart-local"


def evaluate(pair_data: Dataset, catalog: Dataset, output_path: Path) -> dict:
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

    run_id = uuid4().hex[:12]
    agent_name = f"lisa-policy-eval-{run_id}"
    services = build_services(context)
    policy_ids = _add_catalog_to_policy_management(
        context, services.policy_service, catalog, agent_name
    )

    ingest_token = os.getenv("SOFTPROBE_PUBLIC_KEY", "quickstart-local")
    rows = []
    sessions = []
    prepared = []
    for batch in pair_data.manifest.batches:
        record = pair_data.trajectories[batch.trajectory_ref]
        trajectory = record.trajectory.model_dump(mode="json", exclude_unset=True)
        if not (
            trajectory["complete"]
            and trajectory["tool_order_certain"]
            and trajectory["message_order_certain"]
        ):
            sessions.append(
                {
                    "trajectory_ref": batch.trajectory_ref,
                    "status": "capture_gated",
                    "candidate_count": len(policy_ids),
                    "policy_ids": policy_ids,
                }
            )
            for pair in pair_data.pairs(batch, adjudicated_only=True):
                rows.append(
                    {
                        "trajectory_ref": batch.trajectory_ref,
                        "policy_ref": pair.policy.ref,
                        "expected": pair.annotation.outcome,
                        "decision": None,
                        "escalated": None,
                        "verdict": None,
                        "gate_status": "capture_gated",
                    }
                )
            continue

        session_id = f"lisa-eval-{run_id}-{record.id}"
        trace_id, scoped = _run_scoped_ids(run_id, batch.trajectory_ref, trajectory)
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
                f"SessionService reconstructed an incomplete or unordered trajectory for {batch.trajectory_ref}: "
                f"{reconstructed['limitations']}"
            )
        if _trajectory_semantics(reconstructed["events"]) != _trajectory_semantics(
            redact_evidence(scoped["events"])
        ):
            raise RuntimeError(
                f"TheLake changed trajectory event semantics for {batch.trajectory_ref}"
            )

        prepared.append((batch, record, session_id, trace_id))

    # Validate every selected trace through TheLake's production session path before
    # invoking Lisa's production evaluator, so a later fidelity failure cannot leave
    # a partial run of paid Decisions requests.
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
                "trajectory_ref": batch.trajectory_ref,
                "session_id": session_id,
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
                    "trajectory_ref": batch.trajectory_ref,
                    "policy_ref": pair.policy.ref,
                    "annotation_ref": pair.annotation.ref,
                    "expected": pair.annotation.outcome,
                    "decision": decision,
                    "escalated": decision != "no_violation_found",
                    "verdict": verdict,
                    "gate_status": None,
                }
            )

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
    if services.evaluations.policy_triage.client is not None:
        services.evaluations.policy_triage.client.close()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True, help="Scoring pairs/trajectories")
    parser.add_argument(
        "--catalog-manifest",
        type=Path,
        default=Path("lisa/datasets/policy_evaluation/manifests/evaluation.json"),
        help="Full approved-policy catalog; never infer candidates from labeled pairs",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--quickstart-env-file",
        type=Path,
        default=Path("warehouse/quickstart/compose.env"),
        help="Load local DB/API connection metadata created by quickstart",
    )
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--live", action="store_true", help="Allow paid provider calls")
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required to authorize provider calls")
    data_dir = args.data_dir or args.output.parent / f".{args.output.stem}-lisa-data"
    _configure_quickstart_environment(args.quickstart_env_file, data_dir.resolve())
    report = evaluate(load_dataset(args.manifest), load_dataset(args.catalog_manifest), args.output)
    print(json.dumps(report["metrics"], indent=2))


if __name__ == "__main__":
    main()
