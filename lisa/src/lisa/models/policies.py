"""Policy records and the approved rules consumed by evaluations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class PolicyRecord:
    """Persisted policy row, including lifecycle timestamps."""

    id: str
    scope: str
    target_agent_name: str | None
    name: str
    criteria: str
    approved: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None
    archived_at: datetime | None = None


@dataclass(frozen=True)
class PolicyRule:
    """Approved policy passed into triage and judging."""

    name: str
    criteria: str
    approved: bool
    scope: str
    id: str = ""


def policy_id(ordinal: int) -> str:
    """Format a stable policy paragraph reference from its database ordinal."""
    return f"P1.{ordinal}"


def policy_id_sort_key(identifier: str) -> tuple[int, int | str]:
    """Sort paragraph references numerically, then place other IDs afterward."""
    match = re.fullmatch(r"P1\.([1-9][0-9]*)", identifier)
    return (0, int(match.group(1))) if match else (1, identifier)
