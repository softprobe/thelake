"""Filesystem persistence for Lisa's durable workspace conversations."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import shutil
import threading
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any, ClassVar
from uuid import UUID

from .conversation_context import ConversationContextRepository
from .conversation_lock import conversation_lock_path


class ConversationStorageError(ValueError):
    """Base error for invalid or missing conversation storage records."""


class InvalidWorkspaceScope(ConversationStorageError):
    pass


class InvalidConversationId(ConversationStorageError):
    pass


class ConversationNotFound(ConversationStorageError):
    pass


class FileConversationRepository:
    """Own path validation and durable metadata reads/writes."""

    _shared_locks: ClassVar[dict[tuple[str, str], asyncio.Lock]] = {}
    _shared_locks_guard: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir.resolve()

    def workspace_directory(self, workspace_id: str) -> Path:
        try:
            normalized = str(UUID(workspace_id))
        except ValueError as error:
            raise InvalidWorkspaceScope from error
        return self.data_dir / sha256(normalized.encode()).hexdigest()

    def ensure_workspace(self, workspace_id: str) -> Path:
        root = self.workspace_directory(workspace_id)
        root.mkdir(parents=True, exist_ok=True)
        return root

    def conversation_directories(self, workspace_id: str) -> list[Path]:
        root = self.workspace_directory(workspace_id)
        if not root.is_dir():
            return []
        return [
            path
            for path in root.iterdir()
            if path.is_dir() and re.fullmatch(r"[0-9a-f-]{36}", path.name)
        ]

    def metadata_exists(self, directory: Path) -> bool:
        return self.metadata_path(directory).exists()

    @staticmethod
    def delete_conversation(directory: Path) -> None:
        shutil.rmtree(directory)

    def conversation_directory(self, workspace_id: str, conversation_id: str) -> Path:
        try:
            conversation_key = str(UUID(conversation_id))
        except ValueError as error:
            raise InvalidConversationId from error
        path = self.workspace_directory(workspace_id) / conversation_key
        if not path.resolve().is_relative_to(self.data_dir):
            raise InvalidConversationId
        return path

    @staticmethod
    def metadata_path(directory: Path) -> Path:
        return directory / "lisa.json"

    def main_thread_marker(self, workspace_id: str) -> Path:
        return self.workspace_directory(workspace_id) / "main-thread.json"

    def read_main_thread_id(self, marker: Path) -> str:
        try:
            return str(UUID(json.loads(marker.read_text(encoding="utf-8"))["conversation_id"]))
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ConversationNotFound from error

    @staticmethod
    def save_main_thread_id(marker: Path, conversation_id: str) -> None:
        marker.write_text(json.dumps({"conversation_id": conversation_id}), encoding="utf-8")

    def load_metadata(self, directory: Path, workspace_id: str) -> dict[str, Any]:
        try:
            metadata = json.loads(self.metadata_path(directory).read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError) as error:
            raise ConversationNotFound from error
        if (
            metadata.get("workspace_id_hash")
            != sha256(str(UUID(workspace_id)).encode()).hexdigest()
        ):
            raise ConversationNotFound
        return metadata

    def save_metadata(self, directory: Path, metadata: dict[str, Any]) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        target = self.metadata_path(directory)
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
        temporary.replace(target)

    def lock_for(self, directory: Path) -> asyncio.Lock:
        key = (str(self.data_dir), str(directory))
        with self._shared_locks_guard:
            return self._shared_locks.setdefault(key, asyncio.Lock())

    def context_repository(self, directory: Path) -> ConversationContextRepository:
        resolved = directory.resolve()
        if not resolved.is_relative_to(self.data_dir):
            raise InvalidConversationId
        return ConversationContextRepository(
            resolved,
            lock_path=conversation_lock_path(resolved),
            sidecar_lock_path=conversation_lock_path(resolved, "sidecars"),
        )

    @contextmanager
    def process_lock(self, directory: Path) -> Iterator[None]:
        resolved = directory.resolve()
        if not resolved.is_relative_to(self.data_dir):
            raise InvalidConversationId
        lock_path = conversation_lock_path(resolved)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @asynccontextmanager
    async def locked(self, directory: Path):
        """Serialize conversation work across asyncio tasks and local processes."""
        async with self.lock_for(directory):
            loop = asyncio.get_running_loop()
            acquiring = loop.run_in_executor(None, self._acquire_process_lock, directory)
            try:
                descriptor = await asyncio.shield(acquiring)
            except asyncio.CancelledError:
                descriptor = await acquiring
                await asyncio.to_thread(self._release_process_lock, descriptor)
                raise
            try:
                yield
            finally:
                await asyncio.to_thread(self._release_process_lock, descriptor)

    def _acquire_process_lock(self, directory: Path) -> int:
        resolved = directory.resolve()
        if not resolved.is_relative_to(self.data_dir):
            raise InvalidConversationId
        lock_path = conversation_lock_path(resolved)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    @staticmethod
    def _release_process_lock(descriptor: int) -> None:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
