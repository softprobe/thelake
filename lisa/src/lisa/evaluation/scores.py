from __future__ import annotations

import json
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5


EVALUATION_PIPELINE_VERSION = "policy-evaluation-v1"


def evaluation_run_identity(
    *,
    session_id: str,
    policy_hash: str,
    trajectory: dict,
    triage_model: str,
    judge_model: str,
    generation: str = "default",
) -> tuple[str, str, str]:
    """Return trajectory digest, stable score ID, and Postgres request key.

    Per ``docs/architecture/lisa-terms.md``, ``session_id`` identifies the
    runtime input; the trajectory hash fingerprints its derived evidence.
    Hash only the sanitized evidence and configuration that affect triage or judging;
    session score history and unrelated response fields must not change identity.
    """
    evidence = {
        key: trajectory.get(key)
        for key in (
            "trace_ids",
            "events",
            "complete",
            "tool_events_captured",
            "tool_order_certain",
            "message_order_certain",
            "limitations",
        )
    }
    evidence_encoded = json.dumps(
        evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    trajectory_hash = sha256(evidence_encoded.encode("utf-8")).hexdigest()
    identity = {
        "pipeline_version": EVALUATION_PIPELINE_VERSION,
        "session_id": session_id,
        "policy_hash": policy_hash,
        "trajectory_hash": trajectory_hash,
        "triage_model": triage_model,
        "judge_model": judge_model,
        "generation": generation,
    }
    encoded = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    run_digest = sha256(encoded.encode("utf-8")).hexdigest()
    score_id = sha256(f"lisa-evaluation:{run_digest}".encode("utf-8")).hexdigest()
    request_key = str(uuid5(NAMESPACE_URL, f"lisa-evaluation:{run_digest}"))
    return trajectory_hash, score_id, request_key


def matches_policy_score(score: dict, evaluator: dict, trace_id: str) -> bool:
    if score.get("trace_id") != trace_id or score.get("source") != "evaluator":
        return False
    policy_hash = evaluator.get("policy_hash")
    if not policy_hash:
        return False
    return (score.get("metadata") or {}).get("policy_hash") == policy_hash


def successful_policy_score(scores: list[dict], evaluator: dict, trace_id: str) -> dict | None:
    for score in scores:
        if score.get("string_value") == "error":
            continue
        if matches_policy_score(score, evaluator, trace_id):
            return score
    return None
