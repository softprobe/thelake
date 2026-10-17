"""Opt-in, redacted provider-call artifacts, independent of runtime state."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

from .context import WorkspaceContext
from .security import redact_evidence
from .storage import atomic_write_text

logger = logging.getLogger(__name__)
CLEANUP_INTERVAL_SECONDS = 3600
_SCOPE: ContextVar[tuple | None] = ContextVar("lisa_recording_scope", default=None)
_OWNED_FILE = re.compile(r"^[0-9a-f]{32}\.jsonl$")
_REASONING_KEYS = {
    "reasoning",
    "reasoning_content",
    "thinking",
    "thoughts",
    "thought_signature",
    "reasoning_details",
    "encrypted_content",
    "thinking_blocks",
    "reasoning_summary",
    "signature",
}


def safe_data(value):
    """Keep JSON data only; remove private thinking before credential redaction."""

    def private_block(item):
        return isinstance(item, dict) and (
            item.get("type") in {"reasoning", "thinking", "redacted_thinking"}
            or item.get("thought") is True
        )

    def visible(item):
        if private_block(item):
            return None
        if isinstance(item, dict):
            return {
                key: visible(val)
                for key, val in item.items()
                if str(key).lower() not in _REASONING_KEYS
            }
        if isinstance(item, list):
            return [visible(val) for val in item if not private_block(val)]
        if item is None or isinstance(item, (str, bool, int, float)):
            return item
        return None

    return redact_evidence(visible(value))


def current_scope():
    return _SCOPE.get()


def _scope_links(context):
    scope = _SCOPE.get()
    if scope and (scope[0].workspace, scope[0].data_dir.resolve()) == (
        context.workspace,
        context.data_dir.resolve(),
    ):
        return scope[1]
    return {}


@contextmanager
def recording_scope(context: WorkspaceContext | None, **correlation):
    previous = _SCOPE.get()
    context = context or (previous[0] if previous else WorkspaceContext.from_environment())
    links = {**_scope_links(context), **correlation}
    token = _SCOPE.set((context, links))
    try:
        yield
    finally:
        _SCOPE.reset(token)


class RecordedCall:
    def __init__(self, path: Path | None = None, request: dict | None = None):
        self.path, self.request = path, request
        self.root = path.parent.resolve() if path else None
        self.started = time.monotonic()
        self.done = False
        self.lock = threading.Lock()

    def finish(self, response):
        self._terminal("response", response)

    def fail(self, error: BaseException):
        # Exception text/tracebacks may contain full unredactable provider payloads.
        self._terminal("error", {"error_type": type(error).__name__})

    def _terminal(self, kind, value):
        with self.lock:
            if self.done or self.path is None:
                return
            self.done = True
            try:
                row = {
                    "type": kind,
                    "call_id": self.request["call_id"],
                    "timestamp": datetime.now(UTC).isoformat(),
                    "duration_seconds": time.monotonic() - self.started,
                    kind: safe_data(value),
                }
                if (
                    self.path.parent.is_symlink()
                    or self.path.is_symlink()
                    or self.path.parent.resolve() != self.root
                ):
                    raise ValueError("Unsafe recording path")
                atomic_write_text(
                    self.path,
                    "\n".join(json.dumps(item, ensure_ascii=False) for item in [self.request, row])
                    + "\n",
                )
            except Exception:  # noqa: BLE001 - Artifact failures must not change Lisa behavior.
                logger.warning("Lisa call recording failed; continuing without terminal artifact")


class CallRecorder:
    _cleanup_times: ClassVar[dict[tuple[str, str], float]] = {}
    _cleanup_lock = threading.Lock()

    def __init__(self, context: WorkspaceContext):
        self.context = context
        self.enabled = os.getenv("LISA_LLM_TRACE_ENABLED", "").lower() in {"1", "true", "yes"}
        self.cleanup_key = (str(context.data_dir.resolve()), context.workspace)

    @classmethod
    def current(cls):
        scope = _SCOPE.get()
        return cls(scope[0] if scope else WorkspaceContext.from_environment())

    def _root(self):
        home = self.context.home()
        root = home / "llm-calls"
        if (
            home.parent.is_symlink()
            or root.is_symlink()
            or not root.resolve().is_relative_to(home.resolve())
        ):
            raise ValueError("Unsafe recording directory")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root.chmod(0o700)
        return root

    def cleanup(self):
        if not self.enabled:
            return
        try:
            cutoff = time.time() - 30 * 86400
            for path in self._root().iterdir():
                if (
                    _OWNED_FILE.fullmatch(path.name)
                    and not path.is_symlink()
                    and path.is_file()
                    and path.stat().st_mtime < cutoff
                ):
                    path.unlink(missing_ok=True)
            with self._cleanup_lock:
                self._cleanup_times[self.cleanup_key] = time.monotonic()
        except Exception:  # noqa: BLE001 - Artifact failures must not change Lisa behavior.
            logger.warning("Lisa call recording cleanup failed; continuing")

    def start(self, surface, request, **correlation):
        if not self.enabled:
            return RecordedCall()
        try:
            with self._cleanup_lock:
                now = time.monotonic()
                due = (
                    now - self._cleanup_times.get(self.cleanup_key, float("-inf"))
                    > CLEANUP_INTERVAL_SECONDS
                )
                if due:
                    self._cleanup_times[self.cleanup_key] = now
            if due:
                self.cleanup()
            links = {
                **(
                    {"conversation_id": os.environ["LISA_CONVERSATION_ID"]}
                    if os.getenv("LISA_CONVERSATION_ID")
                    else {}
                ),
                **_scope_links(self.context),
                **correlation,
            }
            call_id = uuid4().hex
            row = safe_data(
                {
                    "type": "request",
                    "call_id": call_id,
                    "surface": surface,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "workspace_id": self.context.workspace_id(),
                    "correlation": links,
                    "request": request,
                }
            )
            path = self._root() / f"{call_id}.jsonl"
            atomic_write_text(path, json.dumps(row, ensure_ascii=False) + "\n")
            return RecordedCall(path, row)
        except Exception:  # noqa: BLE001 - Artifact failures must not change Lisa behavior.
            logger.warning("Lisa call recording failed; continuing without request artifact")
            return RecordedCall()


@asynccontextmanager
async def recording_maintenance(context: WorkspaceContext):
    """Run startup and hourly cleanup for HTTP and MCP, even while idle."""
    recorder = CallRecorder(context)
    if not recorder.enabled:
        yield
        return
    await asyncio.to_thread(recorder.cleanup)

    async def maintain():
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
            await asyncio.to_thread(recorder.cleanup)

    task = asyncio.create_task(maintain())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
