"""Trusted workspace configuration supplied by the runtime."""

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
    agent_workspace_root: Path = field(default_factory=lambda: Path("/var/lib/lisa/agent-workspace"))
    database_url: str = field(default="", repr=False)
    database_read_url: str = field(default="", repr=False)
    turn_source: str = "web"
    actor_id: str = ""
    owner_user_ids: frozenset[str] = frozenset()
    conversation_id: str = ""
    chat_turn_id: str = ""
    slack_token: str = field(default="", repr=False)
    slack_channel: str = ""
    slack_thread: str = ""
    activity_slack_channel: str = ""
    activity_slack_thread: str = ""
    thelake_url: str = ""
    thelake_bearer_token: str = field(default="", repr=False)
    thelake_sampling_interval_seconds: int = 3600

    @classmethod
    def from_environment(cls) -> WorkspaceContext:
        return cls(
            workspace=os.getenv("LISA_WORKSPACE_ID", ""),
            data_dir=Path(os.getenv("LISA_DATA_DIR", "/var/lib/lisa/workspaces")).resolve(),
            agent_workspace_root=Path(
                os.getenv("LISA_AGENT_WORKSPACE_ROOT", "/var/lib/lisa/agent-workspace")
            ).resolve(),
            database_url=os.getenv("LISA_DATABASE_URL", ""),
            database_read_url=os.getenv("LISA_DATABASE_READ_URL", ""),
            turn_source=os.getenv("LISA_TURN_SOURCE", "web"),
            actor_id=os.getenv("LISA_ACTOR_ID", "").strip(),
            owner_user_ids=frozenset(
                item.strip() for item in os.getenv("LISA_OWNER_USER_IDS", "").split(",")
                if item.strip()
            ),
            conversation_id=os.getenv("LISA_CONVERSATION_ID", "").strip(),
            chat_turn_id=os.getenv("LISA_CHAT_TURN_ID", "").strip(),
            slack_token=os.getenv("LISA_SLACK_BOT_TOKEN", ""),
            slack_channel=os.getenv("LISA_SLACK_CHANNEL_ID", "").strip(),
            slack_thread=os.getenv("LISA_SLACK_THREAD_TS", "").strip(),
            activity_slack_channel=os.getenv("LISA_SLACK_ACTIVITY_CHANNEL_ID", "").strip(),
            activity_slack_thread=os.getenv("LISA_SLACK_ACTIVITY_THREAD_TS", "").strip(),
            thelake_url=os.getenv("LISA_THELAKE_URL", "").strip(),
            thelake_bearer_token=os.getenv("LISA_THELAKE_BEARER_TOKEN", ""),
            thelake_sampling_interval_seconds=_sampling_interval(),
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


def _sampling_interval() -> int:
    raw = os.getenv("LISA_THELAKE_SAMPLING_INTERVAL_SECONDS", "3600")
    try:
        interval = int(raw)
    except ValueError as error:
        raise ValueError("LISA_THELAKE_SAMPLING_INTERVAL_SECONDS must be an integer") from error
    if interval != 0 and interval < 60:
        raise ValueError("The TheLake sampling interval must be 0 or at least 60 seconds")
    return interval
