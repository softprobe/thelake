"""Atomic notification receipts that prevent duplicate Slack alerts."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from ..context import WorkspaceContext
from ..storage import atomic_write_text, file_lock


class NotificationReceiptRepository:
    def __init__(self, context: WorkspaceContext):
        self.context = context

    def _path(self, evaluator: dict, trace_id: str, channel: str, thread_ts: str | None) -> Path:
        identity = json.dumps(
            [
                evaluator["policy_hash"],
                trace_id,
                channel,
                thread_ts,
            ],
            separators=(",", ":"),
        )
        digest = sha256(identity.encode("utf-8")).hexdigest()
        home = self.context.home()
        directory = home / "notifications"
        if directory.is_symlink() or not directory.resolve().is_relative_to(home.resolve()):
            raise ValueError(
                "Notification receipt path escapes Lisa's workspace home or uses a symlink"
            )
        return directory / f"{digest}.json"

    @contextmanager
    def locked_receipt(
        self, evaluator: dict, trace_id: str, channel: str, thread_ts: str | None
    ) -> Iterator[tuple[bool, Callable[[str, str | None], None]]]:
        path = self._path(evaluator, trace_id, channel, thread_ts)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_suffix(".lock")
        if lock_path.is_symlink():
            raise ValueError("Notification receipt lock cannot be a symlink")
        with file_lock(lock_path):

            def save(session_id: str, message_ts: str | None) -> None:
                atomic_write_text(
                    path,
                    json.dumps({"session_id": session_id, "message_ts": message_ts}),
                )

            yield path.exists(), save
