"""Versioned evaluation inputs and sparse, reviewed pair annotations."""

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


class Record(StrictModel):
    id: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9_-]+$")
    revision: int = Field(ge=1)

    @property
    def ref(self) -> str:
        return f"{self.id}@{self.revision}"


class InputRecord(Record):
    lineage: str = Field(min_length=1)
    provenance: dict[str, str]

    @model_validator(mode="after")
    def named_source(self):
        if not self.provenance.get("source", "").strip():
            raise ValueError("Input provenance requires a source")
        return self


class TrajectoryRecord(InputRecord):
    name: str = Field(min_length=1)
    trajectory: Trajectory


class PolicyRecord(InputRecord):
    policy: PolicySpec


class Review(StrictModel):
    reviewer: str = Field(min_length=1)
    kind: Literal["ai", "human", "author"]
    outcome: Outcome
    scope: str = Field(min_length=1)


class Annotation(Record):
    trajectory_ref: str
    policy_ref: str
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
    trajectory_ref: str
    policy_refs: list[str] = Field(min_length=1)
    annotation_refs: list[str]
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
            run_id=self.annotation.id,
            policy=self.policy.policy,
            trajectory=self.trajectory.trajectory,
        ).model_dump(mode="json", exclude_unset=True)


@dataclass(frozen=True)
class Dataset:
    manifest: Manifest
    digest: str
    trajectories: dict[str, TrajectoryRecord]
    policies: dict[str, PolicyRecord]
    annotations: dict[str, Annotation]

    def batches(self, *, split: str | None = None, source: str | None = None) -> list[Batch]:
        return [
            b
            for b in self.manifest.batches
            if (split is None or b.split == split)
            and (
                source is None or self.trajectories[b.trajectory_ref].provenance["source"] == source
            )
        ]

    def pairs(self, batch: Batch | None = None, *, adjudicated_only: bool = False) -> list[Pair]:
        pairs = []
        for current in [batch] if batch is not None else self.manifest.batches:
            for ref in current.annotation_refs:
                annotation = self.annotations[ref]
                if adjudicated_only and annotation.status != "adjudicated":
                    continue
                pairs.append(
                    Pair(
                        self.trajectories[annotation.trajectory_ref],
                        self.policies[annotation.policy_ref],
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
            if record.ref in indexed:
                raise ValueError(f"Duplicate dataset revision: {record.ref}")
            indexed[record.ref] = record
        records[kind] = indexed
    data = Dataset(manifest, hashlib.sha256(raw).hexdigest(), **records)
    for annotation in data.annotations.values():
        if (
            annotation.trajectory_ref not in data.trajectories
            or annotation.policy_ref not in data.policies
        ):
            raise ValueError(f"Unknown annotation input: {annotation.ref}")
        spans = {e.span_id for e in data.trajectories[annotation.trajectory_ref].trajectory.events}
        if not set(annotation.evidence) <= spans:
            raise ValueError(f"Unknown annotation evidence: {annotation.ref}")
        if annotation.outcome in {"violation", "compliant"} and not annotation.evidence:
            raise ValueError(f"Missing supporting evidence: {annotation.ref}")
    lineage_splits = {}
    used_trajectories = set()
    for batch in manifest.batches:
        if batch.trajectory_ref not in data.trajectories:
            raise ValueError(f"Unknown trajectory: {batch.trajectory_ref}")
        trajectory = data.trajectories[batch.trajectory_ref]
        if lineage_splits.setdefault(trajectory.lineage, batch.split) != batch.split:
            raise ValueError(f"Trajectory lineage crosses splits: {trajectory.lineage}")
        if batch.trajectory_ref in used_trajectories:
            raise ValueError("Duplicate trajectory batch")
        used_trajectories.add(batch.trajectory_ref)
        if len(set(batch.policy_refs)) != len(batch.policy_refs):
            raise ValueError("Duplicate catalog policy")
        if not set(batch.policy_refs) <= data.policies.keys():
            raise ValueError("Unknown catalog policy")
        pairs = set()
        for ref in batch.annotation_refs:
            if ref not in data.annotations:
                raise ValueError(f"Unknown annotation: {ref}")
            annotation = data.annotations[ref]
            if (
                annotation.trajectory_ref != batch.trajectory_ref
                or annotation.policy_ref not in batch.policy_refs
            ):
                raise ValueError(f"Annotation outside batch: {ref}")
            if annotation.policy_ref in pairs:
                raise ValueError("Duplicate annotation for pair")
            pairs.add(annotation.policy_ref)
    return data
