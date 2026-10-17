"""Locked filesystem storage for retryable evaluation failure records."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from ..context import WorkspaceContext
from ..storage import atomic_write_text, file_lock


class FailureRepository:
    def __init__(self, context: WorkspaceContext):
        self.context = context

    def _path(self) -> Path:
        home = self.context.home()
        path = home / "pending-evaluation-failures.json"
        if path.is_symlink() or not path.parent.resolve().is_relative_to(home.resolve()):
            raise ValueError(
                "Pending failure record escapes Lisa's workspace home or uses a symlink"
            )
        return path

    def list_all(self) -> list[dict]:
        try:
            items = json.loads(self._path().read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as error:
            raise ValueError("Lisa's pending evaluation failure record is invalid") from error
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError("Lisa's pending evaluation failure record has an invalid shape")
        return items

    def change(self, operation: Callable[[list[dict]], list[dict]]) -> list[dict]:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_suffix(".lock")
        if lock_path.is_symlink():
            raise ValueError("Pending failure lock cannot be a symlink")
        with file_lock(lock_path):
            updated = operation(self.list_all())
            atomic_write_text(path, json.dumps(updated, ensure_ascii=False))
            return updated
