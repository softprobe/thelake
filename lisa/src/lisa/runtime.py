"""Backend-neutral contract for Lisa conversations."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from .models.policies import is_policy_id


@dataclass(frozen=True)
class AgentTurn:
    messages: list[dict[str, Any]]
    activities: list[dict[str, Any]]
    activity_id: str | None = None


def selected_chat_activity_id(activities: list[dict[str, Any]]) -> str | None:
    """Resolve the activity for a chat turn from explicit choice or activity work."""
    ordered = sorted(activities, key=lambda item: item.get("sequence", 0))

    def tool_name(item: dict[str, Any]) -> str:
        raw = str(item.get("tool_name", "")).lower()
        normalized = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
        known = {
            "attach_chat_turn_to_activity", "update_activity", "set_activity_progress",
            "complete_activity", "set_activity_section", "link_activity_reference",
            "propose_activity_followup", "list_activity_followups",
            "approve_activity_followup", "get_activity_thread", "create_activity",
            "find_activity",
        }
        return next(
            (name for name in known if normalized == name or normalized.endswith(f"_{name}")),
            normalized,
        )

    def tool_arguments(item: dict[str, Any]) -> dict[str, Any]:
        value = item.get("arguments")
        if isinstance(value, dict):
            return value
        if not isinstance(value, str):
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def tool_result(item: dict[str, Any]) -> Any:
        detail = item.get("detail")
        if not isinstance(detail, str):
            return None
        try:
            parsed = json.loads(detail)
        except json.JSONDecodeError:
            # OpenHands prefixes tool output with a human-readable execution
            # marker; parse the JSON payload that follows it.
            decoder = json.JSONDecoder()
            for start, marker in enumerate(detail):
                if marker not in "{[":
                    continue
                try:
                    parsed, _ = decoder.raw_decode(detail[start:])
                    break
                except json.JSONDecodeError:
                    continue
            else:
                return None
        return parsed

    def nonempty_id(value: Any) -> str | None:
        return value.strip() if isinstance(value, str) and value.strip() else None

    explicit = [
        item for item in ordered
        if tool_name(item) == "attach_chat_turn_to_activity"
        and nonempty_id(tool_arguments(item).get("activity_id")) is not None
    ]
    if explicit:
        return nonempty_id(tool_arguments(explicit[-1]).get("activity_id"))

    inferred_ids = {
        nonempty_id(tool_arguments(item).get("activity_id"))
        for item in ordered
        if tool_name(item) in {
            "update_activity", "set_activity_progress", "complete_activity",
            "set_activity_section", "link_activity_reference",
            "propose_activity_followup", "list_activity_followups",
            "approve_activity_followup", "get_activity_thread",
        }
        and nonempty_id(tool_arguments(item).get("activity_id")) is not None
    }
    result_ids: list[str] = []
    for item in ordered:
        name = tool_name(item)
        if item.get("kind") != "evidence" or name not in {
            "create_activity", "find_activity", "get_activity_thread",
        }:
            continue
        payload = tool_result(item)
        if name == "create_activity" and isinstance(payload, dict):
            candidate = payload.get("activity_id")
            candidate = nonempty_id(candidate)
            if candidate:
                result_ids.append(candidate)
        elif name == "get_activity_thread" and isinstance(payload, dict):
            activity = payload.get("activity")
            candidate = activity.get("id") if isinstance(activity, dict) else None
            candidate = nonempty_id(candidate)
            if candidate:
                result_ids.append(candidate)
        elif (
            name == "find_activity" and isinstance(payload, list) and len(payload) == 1
            and isinstance(payload[0], dict)
        ):
            candidate = nonempty_id(payload[0].get("id") or payload[0].get("activity_key"))
            if candidate:
                result_ids.append(candidate)

    inferred_ids.update(result_ids)
    return next(iter(inferred_ids)) if len(inferred_ids) == 1 else None


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
        confirmed_policy_id: str | None = None,
        confirmed_policy_deletion_id: str | None = None,
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


def confirmed_policy_id(user_text: str) -> str | None:
    """Accept approval only as an exact, standalone human-readable policy command."""
    text = _owner_command_text(user_text)
    match = re.fullmatch(r"\s*approve policy:\s*(P1\.[1-9][0-9]*(?:\.[1-9][0-9]*)?)\s*", text, re.IGNORECASE)
    identifier = match.group(1) if match else None
    return identifier if identifier and is_policy_id(identifier) else None


def confirmed_policy_deletion_id(user_text: str) -> str | None:
    """Accept deletion only as an exact, standalone readable policy command."""
    text = _owner_command_text(user_text)
    match = re.fullmatch(r"\s*delete policy:\s*(P1\.[1-9][0-9]*(?:\.[1-9][0-9]*)?)\s*", text, re.IGNORECASE)
    identifier = match.group(1) if match else None
    return identifier if identifier and is_policy_id(identifier) else None


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
