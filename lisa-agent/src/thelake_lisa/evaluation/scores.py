from __future__ import annotations

from hashlib import sha256


def matches_policy_score(score: dict, evaluator: dict, trace_id: str) -> bool:
    if score.get("trace_id") != trace_id or score.get("source") != "evaluator":
        return False
    if evaluator.get("policy_hash"):
        return (score.get("metadata") or {}).get("policy_hash") == evaluator["policy_hash"]
    # History compatibility only: old pending failures may reference an immutable
    # registry version. Never load these definitions to create new evaluations.
    return bool(evaluator.get("evaluator_id")) and score.get("config_id") == (
        f"evaluator:{evaluator['evaluator_id']}:v{evaluator.get('version')}"
    )


def successful_policy_score(scores: list[dict], evaluator: dict, trace_id: str) -> dict | None:
    for score in scores:
        if score.get("string_value") == "error":
            continue
        if matches_policy_score(score, evaluator, trace_id):
            return score
    return None


def next_evaluation_score_id(base_id: str, scores: list[dict]) -> str:
    existing_ids = {score.get("score_id") for score in scores}
    for attempt in range(1, 1000):
        retry_id = sha256(f"{base_id}:retry:{attempt}".encode()).hexdigest()
        if retry_id not in existing_ids:
            return retry_id
    raise RuntimeError("Too many prior evaluation retries for this trace")
