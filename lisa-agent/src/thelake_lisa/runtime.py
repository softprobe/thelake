"""Backend-neutral contract for Lisa conversations."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Protocol


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
        confirmed_policy_title: str | None = None,
        evaluation_approval: tuple[str, str] | None = None,
        activation_approval: tuple[str, str] | None = None,
        slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None,
    ) -> AgentTurn: ...


def backend_name() -> str:
    return os.getenv("LISA_AGENT_BACKEND", "openhands").strip().lower()


def _openhands_backend() -> AgentBackend:
    from thelake_lisa.backends.openhands import OpenHandsBackend

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


def _owner_command_text(user_text: str) -> str:
    """Remove Slack's leading bot mention while preserving exact-command checks."""
    return re.sub(r"^\s*<@[A-Z0-9]+>\s*", "", user_text, count=1, flags=re.IGNORECASE)


def _evaluation_owner_command(user_text: str, action: str) -> tuple[str, str] | None:
    """Bind a trusted owner command to one named rule and exact agent."""
    match = re.fullmatch(
        rf"\s*{action}:\s*(.{1,200}?)\s+for agent:\s*(.{1,256}?)\s*",
        _owner_command_text(user_text),
        re.IGNORECASE,
    )
    return (match.group(1).strip(), match.group(2).strip()) if match else None


def evaluation_approval(user_text: str) -> tuple[str, str] | None:
    """Accept creation only as an exact named-policy and agent command."""
    return _evaluation_owner_command(user_text, "create evaluation")


def activation_approval(user_text: str) -> tuple[str, str] | None:
    """Accept activation only as an exact named-policy and agent command."""
    return _evaluation_owner_command(user_text, "activate evaluation")
