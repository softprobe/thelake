"""Versioned task dataset for Lisa's activity-management agent benchmark.

Offline harness only — see ``datasets`` package docs. Tied to labeled resources; runners live in ``lisa/benchmark``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Literal

from pydantic import Field

from lisa.evaluation.contracts import StrictModel

DATASET_ROOT = Path(__file__).resolve().parent / "activity_management"
DEFAULT_MANIFEST = DATASET_ROOT / "manifests/benchmark.json"


class ActivityTurn(StrictModel):
    chat: Literal["main", "slack"]
    text: str = Field(min_length=1)
    track_activity: bool = True
    actor: Literal["owner", "other"] = "owner"
    channel: str = "C_ACTIVITY_BENCHMARK"
    thread: str = "1781000000.000100"
    require_tools: list[str] = Field(default_factory=list)
    forbid_tools: list[str] = Field(default_factory=list)
    tool_order: list[str] = Field(default_factory=list)
    tool_call_counts: dict[str, int] = Field(default_factory=dict)
    tool_arguments_must_include: list[str] = Field(default_factory=list)
    tool_arguments_must_not_include: list[str] = Field(default_factory=list)
    tool_query_arguments_must_not_include: list[str] = Field(default_factory=list)
    tool_queries_must_filter_sessions: list[str] = Field(default_factory=list)
    expected_tool_errors: list[str] = Field(default_factory=list)
    reply_must_include: list[str] = Field(default_factory=list)
    reply_must_not_include: list[str] = Field(default_factory=list)
    unchanged_activities: list[str] = Field(default_factory=list)


class ActivityExpectation(StrictModel):
    title_contains: str
    status: str | None = None
    next_action_contains: str | None = None
    min_timeline_actions: int = 0
    timeline_text_contains: list[str] = Field(default_factory=list)


class PolicyExpectation(StrictModel):
    name_contains: str
    approved: bool


class ActivityLabels(StrictModel):
    activity_count: int | None = None
    activities: list[ActivityExpectation] = Field(default_factory=list)
    policies: list[PolicyExpectation] = Field(default_factory=list)
    pending_policy_count: int | None = None
    policy_count: int | None = None
    policy_text_contains: list[str] = Field(default_factory=list)
    deleted_policy_ids: list[str] = Field(default_factory=list)


class HistoricalEvaluationSeed(StrictModel):
    session_id: str = Field(min_length=1)
    policy_name: str = Field(min_length=1)
    triage_decision: Literal[
        "suspected_violation", "no_violation_found", "insufficient_evidence"
    ] | None = None
    verdict: Literal["pass", "fail", "uncertain", "insufficient_evidence"] | None = None


class ActivityBenchmarkCase(StrictModel):
    case_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]+$")
    category: Literal["create", "update", "policy", "continuity", "evaluation", "request_resolution", "database"]
    # Agentic held-out split is "benchmark", never "test" (reserved for pytest).
    split: Literal["development", "benchmark"]
    turns: list[ActivityTurn] = Field(min_length=1, max_length=12)
    expected: ActivityLabels
    historical_evaluations: list[HistoricalEvaluationSeed] = Field(default_factory=list)


class ActivityBenchmarkManifest(StrictModel):
    name: str = Field(min_length=1)
    cases_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    case_count: int = Field(ge=1)


def load_activity_dataset(
    manifest_path: Path = DEFAULT_MANIFEST,
) -> tuple[ActivityBenchmarkManifest, list[ActivityBenchmarkCase], str]:
    manifest_path = Path(manifest_path)
    manifest_bytes = manifest_path.read_bytes()
    manifest = ActivityBenchmarkManifest.model_validate(json.loads(manifest_bytes))
    cases_path = manifest_path.parent.parent / "cases.jsonl"
    content = cases_path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != manifest.cases_sha256:
        raise ValueError("Activity benchmark cases digest mismatch")
    cases = [
        ActivityBenchmarkCase.model_validate(json.loads(line))
        for line in content.decode("utf-8").splitlines()
        if line.strip()
    ]
    if len(cases) != manifest.case_count:
        raise ValueError("Activity benchmark case count does not match its manifest")
    ids = [case.case_id for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("Activity benchmark case IDs must be unique")
    return manifest, cases, hashlib.sha256(manifest_bytes).hexdigest()
