"""Trusted workspace configuration and owner approvals supplied by the runtime."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID


@dataclass(frozen=True)
class WorkspaceContext:
    workspace: str
    data_dir: Path
    api_url: str = ""
    api_token: str = field(default="", repr=False)
    database_url: str = field(default="", repr=False)
    database_read_url: str = field(default="", repr=False)
    turn_source: str = "web"
    confirmed_policy_title: str = ""
    slack_token: str = field(default="", repr=False)
    slack_channel: str = ""
    slack_thread: str = ""

    @classmethod
    def from_environment(cls) -> WorkspaceContext:
        return cls(
            workspace=os.getenv("LISA_WORKSPACE_ID", ""),
            data_dir=Path(os.getenv("LISA_DATA_DIR", "/var/lib/lisa/workspaces")).resolve(),
            api_url=os.getenv("LISA_THELAKE_API_URL", "").rstrip("/"),
            api_token=os.getenv("LISA_THELAKE_TOKEN", ""),
            database_url=os.getenv("LISA_DATABASE_URL", ""),
            database_read_url=os.getenv("LISA_DATABASE_READ_URL", ""),
            turn_source=os.getenv("LISA_TURN_SOURCE", "web"),
            confirmed_policy_title=os.getenv("LISA_CONFIRMED_POLICY_TITLE", "").strip(),
            slack_token=os.getenv("LISA_SLACK_BOT_TOKEN", ""),
            slack_channel=os.getenv("LISA_SLACK_CHANNEL_ID", "").strip(),
            slack_thread=os.getenv("LISA_SLACK_THREAD_TS", "").strip(),
        )

    def workspace_id(self) -> str:
        try:
            return str(UUID(self.workspace))
        except ValueError as error:
            raise RuntimeError("Lisa workspace scope is missing or invalid") from error

    def home(self) -> Path:
        data_dir = self.data_dir.resolve()
        digest = hashlib.sha256(self.workspace_id().encode()).hexdigest()
        home = data_dir / digest / "home"
        if home.is_symlink() or not home.resolve().is_relative_to(data_dir):
            raise ValueError("Lisa policy home escapes its workspace data directory")
        return home
