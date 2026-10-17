"""Filesystem persistence and locking for durable evaluation-run artifacts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from ..context import WorkspaceContext
from ..storage import atomic_write_text, file_lock


class EvaluationArtifactRepository:
    """Own evaluation run paths, filesystem safety, locks, and artifact writes."""

    def __init__(self, context: WorkspaceContext):
        self.context = context

    @contextmanager
    def lock(self, policy_hash: str, session_id: str) -> Iterator[None]:
        home = self.context.home()
        directory = home / "evaluation-locks"
        if directory.is_symlink() or not directory.resolve().is_relative_to(home.resolve()):
            raise ValueError("Evaluation lock path escapes Lisa's workspace home or uses a symlink")
        digest = sha256(json.dumps([policy_hash, session_id]).encode()).hexdigest()
        with file_lock(directory / f"{digest}.lock"):
            yield

    def prepare_run_directory(self, relative_path: str, work_item_id: str) -> Path:
        home = self.context.home()
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError("Evaluation context path must stay inside Lisa's workspace home")
        if relative.parts != ("work-items", work_item_id):
            raise ValueError("Evaluation context path does not match its work item")
        root = home / "work-items"
        if root.is_symlink():
            raise ValueError("Evaluation context directory cannot be a symlink")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        run_dir = home / relative
        if run_dir.is_symlink():
            raise ValueError("Evaluation run directory cannot be a symlink")
        if not run_dir.resolve().is_relative_to(root.resolve()):
            raise ValueError("Evaluation context path escapes Lisa's workspace home")
        run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        return run_dir

    @staticmethod
    def save_inputs(run_dir: Path, inputs: dict, context_document: str) -> None:
        serialized = json.dumps(inputs, ensure_ascii=False, sort_keys=True, indent=2)
        atomic_write_text(run_dir / "inputs.json", serialized)
        atomic_write_text(run_dir / "context.md", context_document)
