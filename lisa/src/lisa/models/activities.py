"""Activity values shared by the application and its persistence adapter."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Activity:
    id: str
    created: bool
    context_path: str | None = None
    status: str = "queued"


@dataclass(frozen=True)
class EvaluationRun:
    id: str
    activity_id: str
    session_id: str
    created: bool
    context_path: str | None = None
    status: str = "queued"
    verdict: str | None = None
    score_id: str | None = None
    policy_id: str | None = None


@dataclass(frozen=True)
class ActivityClaim:
    activity_id: str
    owner: str
    fencing_token: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class ActivityEvent:
    activity_id: str
    sequence: int
    event_type: str
    title: str
    summary: str | None
    actor: str
    created_at: datetime


@dataclass(frozen=True)
class ActivityContext:
    id: str
    activity_id: str
    sequence: int
    kind: str
    content: dict[str, Any]
    content_hash: str


@dataclass(frozen=True)
class ScoreOutboxItem:
    id: str
    evaluation_id: str
    status: str
    score_payload: dict[str, Any]
    score_id: str | None
