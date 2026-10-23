"""PGMQ adapter for background conversation checkpoint generation."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

import psycopg
from psycopg.rows import dict_row


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "queue", name).read_text(encoding="utf-8")


class ConversationCheckpointQueue:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def send(self, conversation_id: str, request_id: str) -> tuple[bool, str | None]:
        message = json.dumps(
            {"conversation_id": conversation_id, "request_id": request_id},
            sort_keys=True,
            separators=(",", ":"),
        )
        with psycopg.connect(self.dsn) as connection:
            row = connection.execute(
                _sql("send_conversation_checkpoint.sql"),
                {"message": message, "conversation_id": conversation_id},
            ).fetchone()
        return row[0] is not None, row[1]

    def read_one(self) -> dict[str, Any] | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as connection:
            row = connection.execute(_sql("read_conversation_checkpoint.sql")).fetchone()
        return dict(row) if row else None

    def delete(self, message_id: int) -> None:
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                _sql("delete_conversation_checkpoint.sql"), {"message_id": message_id}
            )

    def archive(self, message_id: int) -> None:
        with psycopg.connect(self.dsn) as connection:
            connection.execute(
                _sql("archive_conversation_checkpoint.sql"), {"message_id": message_id}
            )
