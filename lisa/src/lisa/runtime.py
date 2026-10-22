"""Backend-neutral contract for Lisa conversations."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class AgentTurn:
    messages: list[dict[str, Any]]
    activities: list[dict[str, Any]]
    failed: bool = False


def selected_chat_activity_ids(activities: list[dict[str, Any]]) -> list[str]:
    """Return the activities Lisa explicitly resolved for this chat turn."""
    ordered = sorted(activities, key=lambda item: item.get("sequence", 0))

    def tool_name(item: dict[str, Any]) -> str:
        raw = str(item.get("tool_name", "")).lower()
        return re.sub(r"[^a-z0-9]+", "_", raw).strip("_")

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

    selected: list[str] = []
    for item in ordered:
        if item.get("kind") != "evidence":
            continue
        payload = tool_result(item)
        if not isinstance(payload, dict):
            continue
        name = tool_name(item)
        if name.endswith("complete_activity") and payload.get("status") == "deferred":
            candidate = nonempty_id(payload.get("activity_id"))
            if candidate and candidate not in selected:
                selected.append(candidate)
            continue
        if not name.endswith("resolve_chat_activity"):
            continue
        candidate = nonempty_id(payload.get("activity_id"))
        if (
            candidate
            and payload.get("disposition") in {"create", "continue", "fallback"}
            and candidate not in selected
        ):
            selected.append(candidate)
    return selected


def chat_activity_requested_status(activities: list[dict[str, Any]]) -> str | None:
    """Return the requested terminal outcome from a deferred chat completion."""
    for item in reversed(activities):
        if item.get("kind") != "evidence":
            continue
        name = re.sub(r"[^a-z0-9]+", "_", str(item.get("tool_name", "")).lower()).strip("_")
        if not name.endswith("complete_activity"):
            continue
        payload = _json_payload(item.get("detail"))
        if not isinstance(payload, dict) or payload.get("status") != "deferred":
            continue
        requested = payload.get("requested_status")
        return {"success": "succeeded", "error": "failed", "stopped": "cancelled"}.get(requested)
    return None


def chat_activity_was_skipped(activities: list[dict[str, Any]]) -> bool:
    """Whether Lisa explicitly classified the whole turn as non-work chatter."""
    return any(
        item.get("kind") == "evidence"
        and str(item.get("tool_name", "")).lower().endswith("skip_chat_activity_for_turn")
        and isinstance(_json_payload(item.get("detail")), dict)
        and _json_payload(item.get("detail")).get("skipped") is True
        for item in activities
    )


def record_fallback_activity_resolution(
    activities: list[dict[str, Any]], *, activity_id: str, turn_id: str,
) -> None:
    """Represent runtime recovery as the first trajectory event for this turn."""
    first_sequence = min((item.get("sequence", 0) for item in activities), default=0)
    activities.append({
        "id": f"fallback-resolution:{turn_id}",
        "kind": "evidence",
        "label": "Activity routing fallback: Lisa provided no valid resolution for this request",
        "tool_name": "resolve_chat_activity",
        "detail": json.dumps({
            "disposition": "fallback", "activity_id": activity_id,
            "request_key": "fallback",
        }),
        "sequence": first_sequence - 1,
    })


def chat_activity_waiting_state(
    activities: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """Return whether chat work is pending and the owner action, when known."""
    waiting = False
    next_action: str | None = None
    pending_policies: set[str] = set()
    unidentified_policy_pending = False
    ordered = sorted(activities, key=lambda item: item.get("sequence", 0))
    for item in ordered:
        name = re.sub(
            r"[^a-z0-9]+", "_", str(item.get("tool_name", "")).lower()
        ).strip("_")
        if not name:
            continue
        if name.endswith("set_activity_progress"):
            if item.get("kind") == "tool":
                payload = item.get("arguments")
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        payload = None
            elif item.get("kind") == "evidence":
                payload = _json_payload(item.get("detail"))
            else:
                continue
            if not isinstance(payload, dict):
                continue
            status = payload.get("status")
            value = payload.get("next_action")
            if status == "waiting" or (
                status == "running" and isinstance(value, str) and value.strip()
            ):
                waiting = True
                next_action = (
                    value.strip()
                    if isinstance(value, str) and value.strip()
                    else next_action or "Continue the requested work"
                )
            elif item.get("kind") == "tool" and status in {"queued", "running"}:
                waiting = False
                next_action = None
            continue
        if item.get("kind") != "evidence":
            continue
        payload = _json_payload(item.get("detail"))
        if not isinstance(payload, dict):
            continue
        if name.endswith("write_policy"):
            policy_id = payload.get("policy_id")
            if payload.get("approved") is False:
                if isinstance(policy_id, str) and policy_id:
                    pending_policies.add(policy_id)
                else:
                    unidentified_policy_pending = True
                policy_id = payload.get("policy_id")
                next_action = (
                    f"Approve policy: {policy_id}"
                    if isinstance(policy_id, str) and policy_id
                    else None
                )
            elif payload.get("approved") is True:
                if isinstance(policy_id, str):
                    pending_policies.discard(policy_id)
                else:
                    unidentified_policy_pending = False
                next_action = None
        elif name.endswith("approve_policy") and payload.get("approved") is True:
            policy_id = payload.get("policy_id")
            if isinstance(policy_id, str):
                pending_policies.discard(policy_id)
            else:
                unidentified_policy_pending = False
            next_action = None
        elif name.endswith("delete_policy") and payload.get("deleted") is True:
            policy_id = payload.get("policy_id")
            if isinstance(policy_id, str):
                pending_policies.discard(policy_id)
            else:
                unidentified_policy_pending = False
            next_action = None
    waiting = waiting or bool(pending_policies) or unidentified_policy_pending
    if pending_policies and not next_action:
        next_action = f"Approve policy: {sorted(pending_policies)[0]}"
    if waiting and next_action is None:
        next_action = "Continue the requested work"
    return (
        waiting,
        next_action,
    )


def chat_activity_tool_failed(activities: list[dict[str, Any]]) -> bool:
    """Whether a failed tool call lacks a successful retry with the same inputs."""
    failures: set[tuple[str, str | None]] = set()
    actions_by_id: dict[str, tuple[str, str | None]] = {}
    for item in sorted(activities, key=lambda item: item.get("sequence", 0)):
        name = re.sub(
            r"[^a-z0-9]+", "_", str(item.get("tool_name", "")).lower()
        ).strip("_")
        if item.get("kind") == "tool":
            arguments = item.get("arguments")
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except json.JSONDecodeError:
                    arguments = None
            signature = (
                json.dumps(arguments, sort_keys=True, separators=(",", ":"))
                if isinstance(arguments, dict) else None
            )
            call_id = item.get("tool_call_id")
            if isinstance(call_id, str) and call_id:
                actions_by_id[call_id] = (name, signature)
            continue
        if item.get("kind") != "evidence":
            continue
        detail = str(item.get("detail") or "")
        call_id = item.get("tool_call_id")
        signature = actions_by_id.get(call_id) if isinstance(call_id, str) else None
        signature = signature or (name, None)
        failed = bool(item.get("is_error")) or detail.lstrip().startswith(
            "[An error occurred during execution.]"
        ) or detail.lstrip().startswith("Error calling tool ")
        if failed:
            failures.add(signature)
        else:
            failures.discard(signature)
    return bool(failures)


def activity_events_for_target(
    activities: list[dict[str, Any]], activity_ids: list[str], target_id: str,
) -> list[dict[str, Any]]:
    """Partition runtime events by Lisa's resolved activity scope."""
    ordered = sorted(activities, key=lambda item: item.get("sequence", 0))
    current_scope: str | None = None
    pending_resolution: dict[str, Any] | None = None
    pending_find: dict[str, Any] | None = None
    pre_resolution_event_ids: list[str] = []
    has_resolved_scope = False
    matched: set[str] = set()

    for item in ordered:
        full_name = str(item.get("tool_name", "")).lower()
        is_resolve = full_name.endswith("resolve_chat_activity")
        if is_resolve and item.get("kind") == "tool":
            pending_resolution = item
            explicit = tool_activity_id(item)
            if explicit == target_id:
                matched.add(str(item.get("id", "")))
            continue
        if is_resolve and item.get("kind") == "evidence":
            payload = _json_payload(item.get("detail"))
            resolved = payload.get("activity_id") if isinstance(payload, dict) else None
            if isinstance(resolved, str) and resolved in activity_ids:
                current_scope = resolved
                if not has_resolved_scope and resolved == target_id:
                    matched.update(pre_resolution_event_ids)
                has_resolved_scope = True
                if resolved == target_id:
                    matched.add(str(item.get("id", "")))
                    if pending_resolution is not None:
                        matched.add(str(pending_resolution.get("id", "")))
            pending_resolution = None
            continue

        if not has_resolved_scope and item.get("id"):
            pre_resolution_event_ids.append(str(item["id"]))

        if full_name.endswith("find_activity") and item.get("kind") == "tool":
            pending_find = item
            continue

        direct = tool_activity_id(item)
        if direct is not None:
            if direct == target_id:
                matched.add(str(item.get("id", "")))
            continue
        if item.get("kind") == "evidence" and full_name.endswith("find_activity"):
            payload = _json_payload(item.get("detail"))
            rows = payload if isinstance(payload, list) else [payload]
            if any(
                isinstance(row, dict)
                and (row.get("id") == target_id or row.get("activity_key") == target_id)
                for row in rows
            ):
                matched.add(str(item.get("id", "")))
                if pending_find is not None:
                    matched.add(str(pending_find.get("id", "")))
            pending_find = None
            continue
        if current_scope == target_id:
            matched.add(str(item.get("id", "")))

    return [item for item in ordered if str(item.get("id", "")) in matched]


def tool_activity_id(item: dict[str, Any]) -> str | None:
    arguments = item.get("arguments")
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError:
            return None
    value = arguments.get("activity_id") if isinstance(arguments, dict) else None
    if (
        value is None
        and item.get("kind") == "evidence"
        and str(item.get("tool_name", "")).lower().endswith("complete_activity")
    ):
        payload = _json_payload(item.get("detail"))
        if isinstance(payload, dict) and payload.get("status") == "deferred":
            value = payload.get("activity_id")
    return value.strip() if isinstance(value, str) and value.strip() else None


def _json_payload(detail: Any) -> Any:
    if not isinstance(detail, str):
        return None
    decoder = json.JSONDecoder()
    for start, marker in enumerate(detail):
        if marker not in "{[":
            continue
        try:
            payload, _ = decoder.raw_decode(detail[start:])
            return payload
        except json.JSONDecodeError:
            continue
    return None


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
        slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None,
        chat_turn_id: str | None = None,
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
