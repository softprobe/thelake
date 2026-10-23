"""Durable per-conversation context sidecars and host-local coordination."""

from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from hashlib import sha256
from pathlib import Path
from typing import Any

from .conversation_lock import conversation_lock_path


class ConversationContextRepository:
    """Store checkpoints, checklists, and loaded namespaces beside a chat transcript."""

    def __init__(
        self,
        directory: Path,
        *,
        lock_path: Path | None = None,
        sidecar_lock_path: Path | None = None,
    ):
        self.directory = directory.resolve()
        self.lock_path = (lock_path or conversation_lock_path(self.directory)).resolve()
        self.sidecar_lock_path = (
            sidecar_lock_path or conversation_lock_path(self.directory, "sidecars")
        ).resolve()

    @contextmanager
    def lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @contextmanager
    def sidecar_lock(self) -> Iterator[None]:
        """Coordinate small MCP sidecar writes without blocking a running chat turn."""
        self.sidecar_lock_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.sidecar_lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def write_checkpoint_input(self, request_id: str, payload: dict[str, Any]) -> Path:
        path = self.directory / "context" / "checkpoint-inputs" / f"{request_id}.json"
        self._write_json(path, payload, replace=False)
        return path

    def read_checkpoint_input(self, request_id: str) -> dict[str, Any]:
        return self._read_json(
            self.directory / "context" / "checkpoint-inputs" / f"{request_id}.json"
        )

    def delete_checkpoint_input(self, request_id: str) -> None:
        path = self.directory / "context" / "checkpoint-inputs" / f"{request_id}.json"
        path.unlink(missing_ok=True)

    def publish_checkpoint(
        self,
        request_id: str,
        checkpoint: dict[str, Any],
        *,
        lock_held: bool = False,
    ) -> dict[str, Any]:
        checkpoint_dir = self.directory / "context" / "checkpoints"
        path = checkpoint_dir / f"{request_id}.json"
        config_fingerprint = checkpoint.get("config_fingerprint")
        if not isinstance(config_fingerprint, str) or not re.fullmatch(
            r"[0-9a-f]{64}", config_fingerprint
        ):
            raise ValueError("Checkpoint is missing a valid configuration fingerprint")
        pointer = checkpoint_dir / "current" / f"{config_fingerprint}.json"
        with nullcontext() if lock_held else self.lock():
            if path.is_file():
                value = self._read_json(path)
            else:
                value = {**checkpoint, "request_id": request_id}
                self._write_json(path, value)
            current = self.current_checkpoint(config_fingerprint)
            candidate_time = str(value.get("captured_at", ""))
            current_time = str(current.get("captured_at", "")) if current else ""
            if current is None or candidate_time >= current_time:
                self._write_json(pointer, {"request_id": request_id})
            return value

    def current_checkpoint(self, config_fingerprint: str) -> dict[str, Any] | None:
        if not re.fullmatch(r"[0-9a-f]{64}", config_fingerprint):
            raise ValueError("Checkpoint configuration fingerprint is invalid")
        pointer = (
            self.directory / "context" / "checkpoints" / "current" / f"{config_fingerprint}.json"
        )
        if not pointer.is_file():
            return None
        request_id = self._read_json(pointer).get("request_id")
        if not isinstance(request_id, str):
            return None
        try:
            return self._read_json(
                self.directory / "context" / "checkpoints" / f"{request_id}.json"
            )
        except FileNotFoundError:
            return None

    def read_checkpoint(self, request_id: str) -> dict[str, Any] | None:
        try:
            return self._read_json(
                self.directory / "context" / "checkpoints" / f"{request_id}.json"
            )
        except FileNotFoundError:
            return None

    def usable_checkpoint(
        self,
        event_ids: list[str],
        config_fingerprint: str,
        prefix_fingerprint: str | None = None,
    ) -> dict[str, Any] | None:
        candidate = self.current_checkpoint(config_fingerprint)
        if candidate is None or candidate.get("config_fingerprint") != config_fingerprint:
            return None
        prefix_ids = candidate.get("prefix_event_ids")
        if not isinstance(prefix_ids, list) or not prefix_ids:
            return None
        prefix = [str(item) for item in prefix_ids]
        if event_ids[: len(prefix)] != prefix:
            return None
        if (
            prefix_fingerprint is not None
            and candidate.get("prefix_fingerprint") != prefix_fingerprint
        ):
            return None
        return candidate

    def read_todo(self) -> list[dict[str, Any]]:
        path = self.directory / "context" / "todo.json"
        try:
            value = self._read_json(path)
        except FileNotFoundError:
            return []
        items = value.get("items", [])
        return items if isinstance(items, list) else []

    def write_todo(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        active = [
            item
            for item in items
            if isinstance(item, dict)
            and item.get("status", "open") not in {"completed", "cancelled"}
        ]
        with self.sidecar_lock():
            self._write_json(self.directory / "context" / "todo.json", {"items": active})
        return active

    def loaded_namespaces(self) -> set[str]:
        path = self.directory / "context" / "loaded-namespaces.json"
        try:
            namespaces = self._read_json(path).get("namespaces", [])
        except FileNotFoundError:
            return set()
        return {item for item in namespaces if isinstance(item, str)}

    def set_loaded_namespaces(self, namespaces: set[str]) -> None:
        with self.sidecar_lock():
            self._write_json(
                self.directory / "context" / "loaded-namespaces.json",
                {"namespaces": sorted(namespaces)},
            )

    def clear_loaded_namespaces(self) -> None:
        self.set_loaded_namespaces(set())

    def _write_json(self, path: Path, value: dict[str, Any], *, replace: bool = True) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(serialized)
                stream.flush()
                os.fsync(stream.fileno())
            temporary = Path(temporary_name)
            if replace:
                os.replace(temporary, path)
            else:
                try:
                    # A hard link publishes the complete file atomically and
                    # fails if another worker already published this key.
                    os.link(temporary, path)
                except FileExistsError:
                    return
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            Path(temporary_name).unlink(missing_ok=True)

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError(f"Expected an object in {path.name}")
        return value


def checkpoint_request_id(config_fingerprint: str, prefix_fingerprint: str) -> str:
    return sha256(f"{config_fingerprint}:{prefix_fingerprint}".encode()).hexdigest()
