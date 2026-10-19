"""Backend-neutral contract for Lisa conversations."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True)
class AgentTurn:
    messages: list[dict[str, Any]]
    activities: list[dict[str, Any]]


class AgentBackend(Protocol):
    """Runs one turn while preserving the backend's durable conversation state."""

    def run_turn(
        self,
        *,
        conversation_id: str,
        directory: str,
        workspace_id: str,
        text: str,
        turn_source: str = "web",
        actor_id: str | None = None,
        confirmed_policy_title: str | None = None,
        confirmed_policy_proposal_id: str | None = None,
        rejected_policy_proposal_id: str | None = None,
        confirmed_followup_id: str | None = None,
        slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None,
    ) -> AgentTurn: ...


def backend_name() -> str:
    return os.getenv("LISA_AGENT_BACKEND", "openhands").strip().lower()


def _openhands_backend() -> AgentBackend:
    from lisa.backends.openhands import OpenHandsBackend

    return OpenHandsBackend()


def get_backend() -> AgentBackend:
    name = backend_name()
    if name == "openhands":
        return _openhands_backend()
    raise RuntimeError(f"Unsupported Lisa agent backend: {name}")


def initialize_backend() -> None:
    """Load backend SDK dependencies before the service accepts concurrent turns."""
    backend = get_backend()
    prepare = getattr(backend, "prepare", None)
    if callable(prepare):
        prepare()


def run_turn(**kwargs: Any) -> AgentTurn:
    return get_backend().run_turn(**kwargs)


def confirmed_policy_title(user_text: str) -> str | None:
    """Accept an owner confirmation only as an exact, standalone chat command."""
    text = _owner_command_text(user_text)
    match = re.fullmatch(r"\s*confirm policy rule:\s*(.{1,200}?)\s*", text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def confirmed_policy_proposal_id(user_text: str) -> str | None:
    """Accept an exact proposal-ID confirmation from an owner conversation."""
    text = _owner_command_text(user_text)
    match = re.fullmatch(
        r"\s*confirm policy proposal:\s*([0-9a-f-]{36})\s*", text, re.IGNORECASE
    )
    if not match:
        return None
    try:
        return str(UUID(match.group(1)))
    except ValueError:
        return None


def rejected_policy_proposal_id(user_text: str) -> str | None:
    """Accept an exact rejection of one proposal in an owner conversation."""
    text = _owner_command_text(user_text)
    match = re.fullmatch(
        r"\s*reject policy proposal:\s*([0-9a-f-]{36})\s*", text, re.IGNORECASE
    )
    if not match:
        return None
    try:
        return str(UUID(match.group(1)))
    except ValueError:
        return None


def confirmed_followup_id(user_text: str) -> str | None:
    """Accept an exact follow-up ID confirmation from an owner conversation."""
    text = _owner_command_text(user_text)
    match = re.fullmatch(r"\s*confirm follow-up:\s*([0-9a-f-]{36})\s*", text, re.IGNORECASE)
    if not match:
        return None
    try:
        return str(UUID(match.group(1)))
    except ValueError:
        return None


def _owner_command_text(user_text: str) -> str:
    """Remove Slack's leading bot mention while preserving exact-command checks."""
    return re.sub(r"^\s*<@[A-Z0-9]+>\s*", "", user_text, count=1, flags=re.IGNORECASE)
