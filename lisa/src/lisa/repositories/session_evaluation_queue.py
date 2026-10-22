"""PGMQ adapter for durable evaluation requests derived from pushed sessions."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

import psycopg
from psycopg.rows import dict_row


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "queue", name).read_text(encoding="utf-8")


class SessionEvaluationQueue:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def send(self, session_id: str, agent_name: str, payload_hash: str) -> bool:
        message = json.dumps(
            {
                "session_id": session_id,
                "agent_name": agent_name,
                "payload_hash": payload_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as connection, connection.transaction():
            inserted = connection.execute(
                _sql("register_session_evaluation.sql"),
                {"session_id": session_id, "payload_hash": payload_hash},
            ).fetchone()
            if inserted is None:
                existing = connection.execute(
                    _sql("get_session_evaluation_receipt.sql"),
                    {"session_id": session_id},
                ).fetchone()
                if existing is None or existing["payload_hash"] != payload_hash:
                    raise ValueError("Session ID is already queued with different evidence")
                return False
            connection.execute(
                _sql("send_session_evaluation.sql"), {"message": message}
            )
            return True

    def read_one(self) -> dict[str, Any] | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as connection:
            row = connection.execute(_sql("read_session_evaluation.sql")).fetchone()
        return dict(row) if row else None

    def delete(self, message_id: int) -> None:
        with psycopg.connect(self.dsn) as connection:
            connection.execute(_sql("delete_session_evaluation.sql"), {"message_id": message_id})

    def archive(self, message_id: int) -> None:
        with psycopg.connect(self.dsn) as connection:
            connection.execute(_sql("archive_session_evaluation.sql"), {"message_id": message_id})
