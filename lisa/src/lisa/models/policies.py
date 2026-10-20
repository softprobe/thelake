"""Policy records and the approved rules consumed by evaluations.

See ``docs/architecture/lisa-terms.md`` for the meaning of Lisa's readable
policy ID and its role in a session-policy evaluation pair.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

POLICY_ID_PATTERN = re.compile(r"P1\.([1-9][0-9]*)(?:\.([1-9][0-9]*))?")


@dataclass(frozen=True)
class PolicyRecord:
    """One current policy row, identified by its readable section ID."""

    id: str
    scope: str
    target_agent_name: str | None
    name: str
    criteria: str
    approved: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class PolicyRule:
    """Approved policy passed into triage and judging."""

    name: str
    criteria: str
    approved: bool
    scope: str
    id: str = ""


def policy_id(ordinal: int) -> str:
    """Format a stable human-readable policy ID from its database ordinal."""
    return f"P1.{ordinal}"


def is_policy_id(identifier: str) -> bool:
    """Return whether an identifier is a supported readable policy reference."""
    return POLICY_ID_PATTERN.fullmatch(identifier) is not None


def policy_id_sort_key(identifier: str) -> tuple[int, int | str, int]:
    """Sort policy IDs numerically, then place other IDs afterward."""
    match = POLICY_ID_PATTERN.fullmatch(identifier)
    return (0, int(match.group(1)), int(match.group(2) or 0)) if match else (1, identifier, 0)
