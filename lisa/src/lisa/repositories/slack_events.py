"""Durable receipts for Slack event delivery and replay protection."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from .conversations import FileConversationRepository


class SlackEventRepository:
    def __init__(self, conversations: FileConversationRepository):
        self.conversations = conversations

    def event_path(self, workspace_id: str, event_id: str) -> Path:
        digest = sha256(event_id.encode("utf-8")).hexdigest()
        directory = self.conversations.workspace_directory(workspace_id) / "slack_events"
        if directory.is_symlink() or not directory.resolve().is_relative_to(
            self.conversations.data_dir
        ):
            raise ValueError("Slack event path escapes Lisa's workspace data directory")
        return directory / f"{digest}.json"

    @staticmethod
    def load(path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def exists(path: Path) -> bool:
        return path.exists()

    @staticmethod
    def save(path: Path, event: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(event, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)

    def pending_paths(self, workspace_id: str) -> list[Path]:
        directory = self.conversations.workspace_directory(workspace_id) / "slack_events"
        if not directory.is_dir() or directory.is_symlink():
            return []
        return sorted(directory.glob("*.json"))
