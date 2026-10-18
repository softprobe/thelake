"""Evaluation inputs and sparse, reviewed session-policy annotations.

See ``docs/architecture/lisa-terms.md`` for canonical runtime identity. The
dataset stores trajectories by ``session_id`` and annotations by
``(session_id, policy_id)``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .contracts import EvaluationRequest, PolicySpec, StrictModel, Trajectory

Outcome = Literal["violation", "compliant", "not_applicable", "insufficient_evidence"]
DEFAULT_MANIFEST = (
    Path(__file__).resolve().parents[3] / "datasets/policy_evaluation/manifests/evaluation.json"
)


class InputRecord(StrictModel):
    lineage: str = Field(min_length=1)
    provenance: dict[str, str]

    @model_validator(mode="after")
    def named_source(self):
        if not self.provenance.get("source", "").strip():
            raise ValueError("Input provenance requires a source")
        return self


class TrajectoryRecord(InputRecord):
    session_id: str = Field(min_length=1, max_length=256)
    name: str = Field(min_length=1)
    trajectory: Trajectory


class PolicyRecord(InputRecord):
    policy_id: str = Field(pattern=r"^P1\.[1-9][0-9]*$")
    policy: PolicySpec


class Review(StrictModel):
    reviewer: str = Field(min_length=1)
    kind: Literal["ai", "human", "author"]
    outcome: Outcome
    scope: str = Field(min_length=1)


class Annotation(StrictModel):
    session_id: str = Field(min_length=1, max_length=256)
    policy_id: str = Field(pattern=r"^P1\.[1-9][0-9]*$")
    outcome: Outcome
    rationale: str = Field(min_length=1)
    evidence: list[str]
    status: Literal["proposed", "disputed", "adjudicated"]
    reviews: list[Review]

    @model_validator(mode="after")
    def reviewed_adjudication(self):
        if self.status == "adjudicated" and not self.reviews:
            raise ValueError("Adjudicated annotations require review provenance")
        return self


class Batch(StrictModel):
    session_id: str = Field(min_length=1, max_length=256)
    policy_ids: list[str] = Field(min_length=1)
    split: Literal["development", "test"]


class Manifest(StrictModel):
    name: str = Field(min_length=1)
    files: dict[Literal["trajectories", "policies", "annotations"], str]
    batches: list[Batch]


@dataclass(frozen=True)
class Pair:
    trajectory: TrajectoryRecord
    policy: PolicyRecord
    annotation: Annotation

    def request(self) -> dict:
        return EvaluationRequest(
            run_id=f"{self.trajectory.session_id}:{self.policy.policy_id}",
            policy=self.policy.policy,
            trajectory=self.trajectory.trajectory,
        ).model_dump(mode="json", exclude_unset=True)


@dataclass(frozen=True)
class Dataset:
    manifest: Manifest
    digest: str
    trajectories: dict[str, TrajectoryRecord]
    policies: dict[str, PolicyRecord]
    annotations: dict[tuple[str, str], Annotation]

    def batches(self, *, split: str | None = None, source: str | None = None) -> list[Batch]:
        return [
            b
            for b in self.manifest.batches
            if (split is None or b.split == split)
            and (
                source is None or self.trajectories[b.session_id].provenance["source"] == source
            )
        ]

    def pairs(self, batch: Batch | None = None, *, adjudicated_only: bool = False) -> list[Pair]:
        pairs = []
        for current in [batch] if batch is not None else self.manifest.batches:
            for policy_id in current.policy_ids:
                annotation = self.annotations.get((current.session_id, policy_id))
                if annotation is None:
                    continue
                if adjudicated_only and annotation.status != "adjudicated":
                    continue
                pairs.append(
                    Pair(
                        self.trajectories[annotation.session_id],
                        self.policies[annotation.policy_id],
                        annotation,
                    )
                )
        return pairs


def _json(text: str):
    def invalid(value):
        raise ValueError(f"Nonfinite JSON value: {value}")

    return json.loads(text, parse_constant=invalid)


def load_dataset(path: Path = DEFAULT_MANIFEST) -> Dataset:
    """Resolve a frozen manifest. Missing pair annotations remain missing."""
    path = Path(path)
    raw = path.read_bytes()
    manifest = Manifest.model_validate(_json(raw.decode()))
    if set(manifest.files) != {"trajectories", "policies", "annotations"}:
        raise ValueError("Manifest must pin all three dataset files")
    records = {}
    for kind, model in [
        ("trajectories", TrajectoryRecord),
        ("policies", PolicyRecord),
        ("annotations", Annotation),
    ]:
        content = (path.parent.parent / f"{kind}.jsonl").read_bytes()
        if hashlib.sha256(content).hexdigest() != manifest.files[kind]:
            raise ValueError(f"Dataset digest mismatch: {kind}")
        indexed = {}
        for line in content.decode().splitlines():
            record = model.model_validate(_json(line))
            if kind == "trajectories":
                key = record.session_id
            elif kind == "policies":
                key = record.policy_id
            else:
                key = (record.session_id, record.policy_id)
            if key in indexed:
                if kind == "annotations":
                    raise ValueError("Duplicate annotation for pair")
                raise ValueError(f"Duplicate dataset ID: {key}")
            indexed[key] = record
        records[kind] = indexed
    data = Dataset(manifest, hashlib.sha256(raw).hexdigest(), **records)
    for annotation in data.annotations.values():
        if (
            annotation.session_id not in data.trajectories
            or annotation.policy_id not in data.policies
        ):
            raise ValueError(
                f"Unknown annotation input: ({annotation.session_id}, {annotation.policy_id})"
            )
        spans = {e.span_id for e in data.trajectories[annotation.session_id].trajectory.events}
        if not set(annotation.evidence) <= spans:
            raise ValueError(
                f"Unknown annotation evidence: ({annotation.session_id}, {annotation.policy_id})"
            )
        if annotation.outcome in {"violation", "compliant"} and not annotation.evidence:
            raise ValueError(
                f"Missing supporting evidence: ({annotation.session_id}, {annotation.policy_id})"
            )
    lineage_splits = {}
    used_trajectories = set()
    for batch in manifest.batches:
        if batch.session_id not in data.trajectories:
            raise ValueError(f"Unknown session: {batch.session_id}")
        trajectory = data.trajectories[batch.session_id]
        if lineage_splits.setdefault(trajectory.lineage, batch.split) != batch.split:
            raise ValueError(f"Trajectory lineage crosses splits: {trajectory.lineage}")
        if batch.session_id in used_trajectories:
            raise ValueError("Duplicate session batch")
        used_trajectories.add(batch.session_id)
        if len(set(batch.policy_ids)) != len(batch.policy_ids):
            raise ValueError("Duplicate catalog policy")
        if not set(batch.policy_ids) <= data.policies.keys():
            raise ValueError("Unknown catalog policy")
        for annotation in data.annotations.values():
            if annotation.session_id == batch.session_id and annotation.policy_id not in batch.policy_ids:
                raise ValueError(
                    f"Annotation outside batch: ({annotation.session_id}, {annotation.policy_id})"
                )
    batched_sessions = {batch.session_id for batch in manifest.batches}
    if any(annotation.session_id not in batched_sessions for annotation in data.annotations.values()):
        raise ValueError("Annotation outside manifest batches")
    return data
