"""Work-item values shared by the application and its persistence adapter."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class EvaluationWorkItem:
    id: str
    session_id: str
    created: bool
    context_path: str | None = None
    status: str = "queued"
    verdict: str | None = None
    score_id: str | None = None
    policy_id: str | None = None


@dataclass(frozen=True)
class WorkItemClaim:
    work_item_id: str
    owner: str
    fencing_token: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class WorkEvent:
    work_item_id: str
    sequence: int
    event_type: str
    title: str
    summary: str | None
    actor: str
    created_at: datetime


@dataclass(frozen=True)
class WorkItemContext:
    id: str
    work_item_id: str
    sequence: int
    kind: str
    content: dict[str, Any]
    content_hash: str


@dataclass(frozen=True)
class ScoreOutboxItem:
    id: str
    work_item_id: str
    status: str
    score_payload: dict[str, Any]
    score_id: str | None
