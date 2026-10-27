"""Durable, source-linked updates shown in Lisa's primary chat timeline."""

from __future__ import annotations

import json
from importlib.resources import files
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from ..security import redact_text


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "activities", name).read_text(encoding="utf-8")


def create_proactive_update(
    connection,
    *,
    source_type: str,
    source_id: str,
    messages: list[str],
    activity_id: str | None = None,
) -> None:
    """Create one source-linked, model-authored chat update transactionally."""
    if source_type not in {"policy_suggestion", "policy_finding", "qa_observation"}:
        raise ValueError("Unsupported proactive update source")
    try:
        source_id = str(UUID(source_id))
        activity_id = str(UUID(activity_id)) if activity_id is not None else None
    except ValueError:
        raise ValueError("Proactive update source IDs must be UUIDs") from None
    if (source_type == "policy_suggestion" and activity_id is not None) or (
        source_type == "policy_finding" and activity_id != source_id
    ) or (source_type == "qa_observation" and activity_id is None):
        raise ValueError("Proactive update source reference does not match its type")
    if not messages or any(not message.strip() for message in messages):
        raise ValueError("Proactive updates require model-authored messages")
    safe_messages = [redact_text(message)[:4_000] for message in messages]
    connection.execute(_sql("create_proactive_update.sql"), {
        "id": str(uuid4()),
        "source_type": source_type,
        "source_id": source_id,
        "messages": json.dumps(safe_messages),
        "activity_id": activity_id,
    })


class ProactiveUpdateRepository:
    """Read bounded update references from this workspace-bound Lisa database."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def list(self, *, limit: int = 20) -> list[dict]:
        if not 1 <= limit <= 100:
            raise ValueError("Proactive update limit must be between 1 and 100")
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_proactive_updates.sql"), {"limit": limit}
            ).fetchall()
        return [
            {
                "id": str(row["id"]),
                "source_type": row["source_type"],
                "source_id": row["source_id"],
                "messages": row["messages"],
                "activity_id": str(row["activity_id"]) if row["activity_id"] else None,
                "created_at": row["created_at"].isoformat(),
            }
            for row in rows
        ]
