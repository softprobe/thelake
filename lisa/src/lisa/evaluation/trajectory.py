"""Build compact session trajectories from TheLake span telemetry.

Terms used in Lisa evaluation:

- ``session_detail``: raw ``GET /v1/sessions/{id}`` payload (spans, scores,
  agent names, truncation flags).
- ``trajectory``: ordered user/tool/assistant events plus completeness
  flags used for policy triage and judging.
- ``references``: span citations on a judge verdict (not the trajectory).
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SESSION_SPAN_LIMIT = 500


@dataclass
class TrajectoryEvents:
    events: list[dict]
    tool_events_captured: bool
    tool_order_certain: bool
    limitations: list[str] = field(default_factory=list)
    message_order_certain: bool = True

    @property
    def complete(self) -> bool:
        return not self.limitations


def build_trajectory_events(spans: list[dict]) -> TrajectoryEvents:
    """Project session spans into ordered trajectory events for triage/judging.

    Generation spans often repeat the full prompt, system instructions, tool
    schemas, and usage metadata. Agent spans carry the conversational turns,
    while tool spans carry the actual arguments and result; those are the
    smallest records that preserve what a policy judge needs to assess.
    """
    limitations: list[str] = []
    raw_events: list[tuple[str, str, dict]] = []
    if len(spans) > 1000:
        limitations.append("Session spans exceeded the 1000 span limit.")
    tool_names = [name for span in spans if (name := _tool_name(span))]
    idless_tool_name_counts = Counter(
        name for span in spans if _tool_call_id(span) is None and (name := _tool_name(span))
    )
    matched_idless_tool_names: Counter[str] = Counter()
    tool_call_ids = {str(call_id) for span in spans if (call_id := _tool_call_id(span)) is not None}
    tool_events_captured = bool(tool_names)
    tool_order_certain = True
    # Missing timestamps sort deterministically before observed timestamps, while
    # the projected events retain null to distinguish inferred time from evidence.
    fallback_time = ""

    for span in spans:
        span_id = str(span.get("span_id") or "")
        if not span_id:
            continue
        parent_span_id = span.get("parent_span_id")
        has_start_time = bool(span.get("start_time"))
        has_end_time = bool(span.get("end_time"))
        start = str(span.get("start_time") or fallback_time)
        end = str(span.get("end_time") or start)
        event_start = start if has_start_time else None
        event_end = end if has_end_time else event_start
        attrs = span.get("attributes") if isinstance(span.get("attributes"), dict) else {}
        tool_name = _tool_name(span)

        if tool_name:
            call_id = attrs.get("gen_ai.tool.call.id")
            raw_events.append(
                (
                    start,
                    span_id,
                    {
                        "span_id": span_id,
                        "parent_span_id": parent_span_id,
                        "timestamp": event_start,
                        "kind": "tool_call",
                        "name": str(tool_name),
                        "call_id": call_id,
                        "payload": _payload(attrs.get("sp.input", span.get("input"))),
                    },
                )
            )
            if span.get("end_time"):
                result = {
                    "span_id": span_id,
                    "parent_span_id": parent_span_id,
                    "timestamp": event_end,
                    "kind": "tool_result",
                    "name": str(tool_name),
                    "call_id": call_id,
                    "payload": _payload(attrs.get("sp.output", span.get("output"))),
                }
                status = attrs.get("sp.tool.status") or span.get("status_code")
                if status:
                    result["status"] = str(status)
                raw_events.append((end, span_id, result))
            continue

        prompt = attrs.get("sp.input", span.get("input"))
        for message in _messages(prompt, limitations):
            if message["kind"] in {"user_message", "context_message"}:
                raw_events.append(
                    (start, span_id, _event(span_id, parent_span_id, event_start, message))
                )

        completion = attrs.get("sp.output", span.get("output"))
        for message in _messages(completion, limitations, output=True):
            if message["kind"] == "assistant_message":
                raw_events.append(
                    (end, span_id, _event(span_id, parent_span_id, event_end, message))
                )
            elif message["kind"] == "tool_call":
                # The tool span is the authoritative call record: it includes
                # the arguments that actually reached the tool. Keep a model
                # call only when telemetry did not capture a matching tool span.
                name = str(message.get("name") or "")
                call_id = message.get("call_id")
                if call_id and str(call_id) in tool_call_ids:
                    continue
                if (
                    not call_id
                    and name
                    and matched_idless_tool_names[name] < idless_tool_name_counts[name]
                ):
                    matched_idless_tool_names[name] += 1
                    continue
                tool_events_captured = True
                limitations.append(
                    f"Tool call {message.get('name', 'unknown')} has no matching tool span."
                )
                raw_events.append(
                    (end, span_id, _event(span_id, parent_span_id, event_end, message))
                )

    # Parent and child telemetry can both carry the same transcript. Suppress
    # only child copies already present in an ancestor span; identical messages
    # within one span or in separate turns are behavior and must be preserved.
    raw_events.sort(key=lambda item: (item[0], item[1]))
    parent_by_span = {
        str(span.get("span_id")): str(span.get("parent_span_id"))
        for span in spans
        if span.get("span_id")
    }
    message_times_by_span: dict[str, dict[tuple[str, str], list[str]]] = {}
    for timestamp, span_id, event in raw_events:
        if event["kind"] in {"user_message", "assistant_message", "context_message"}:
            identity = (event["kind"], str(event.get("content") or ""))
            message_times_by_span.setdefault(span_id, {}).setdefault(identity, []).append(timestamp)
    events: list[tuple[str, str, dict]] = []
    occurrences_by_span: dict[str, Counter[tuple[str, str]]] = {}
    for timestamp, span_id, event in raw_events:
        if event["kind"] in {"user_message", "assistant_message", "context_message"}:
            identity = (event["kind"], str(event.get("content") or ""))
            occurrences = occurrences_by_span.setdefault(span_id, Counter())
            occurrences[identity] += 1
            ancestor_id = parent_by_span.get(span_id)
            ancestor_count = 0
            visited: set[str] = set()
            while ancestor_id and ancestor_id not in visited:
                visited.add(ancestor_id)
                ancestor_times = message_times_by_span.get(ancestor_id, {}).get(identity, [])
                ancestor_count = max(
                    ancestor_count,
                    sum(
                        _near_same_time(timestamp, ancestor_time)
                        for ancestor_time in ancestor_times
                    ),
                )
                ancestor_id = parent_by_span.get(ancestor_id)
            if occurrences[identity] <= ancestor_count:
                continue
        events.append((timestamp, span_id, event))

    if len(events) > 1000:
        limitations.append("Session events exceeded the 1000 event limit.")
    normalized = []
    previous_tool_timestamp = None
    message_spans_by_time: dict[str | None, set[str]] = {}
    for sequence, (timestamp, span_id, event) in enumerate(events[:1000]):
        if event["kind"] in {"tool_call", "tool_result"}:
            if timestamp == previous_tool_timestamp:
                tool_order_certain = False
            previous_tool_timestamp = timestamp
        if event["kind"] in {"user_message", "assistant_message", "context_message"}:
            message_spans_by_time.setdefault(event.get("timestamp"), set()).add(span_id)
        normalized.append({"sequence": sequence, **event})
    message_span_ids = {
        span_id for span_ids in message_spans_by_time.values() for span_id in span_ids
    }
    if None in message_spans_by_time and len(message_span_ids) > 1:
        message_order_certain = False
    tool_events = [
        event for _, _, event in events if event["kind"] in {"tool_call", "tool_result"}
    ]
    tool_span_ids = {event["span_id"] for event in tool_events}
    if any(event["timestamp"] is None for event in tool_events) and len(tool_span_ids) > 1:
        tool_order_certain = False
    message_order_certain = all(
        len(spans) == 1 for spans in message_spans_by_time.values()
    ) and not (None in message_spans_by_time and len(message_span_ids) > 1)
    return TrajectoryEvents(
        normalized,
        tool_events_captured,
        tool_order_certain,
        list(dict.fromkeys(limitations)),
        message_order_certain=message_order_certain,
    )


def _near_same_time(left: str, right: str) -> bool:
    """Treat only near-simultaneous parent/child copies as the same turn."""
    if not left and not right:
        return True
    try:
        left_time = datetime.fromisoformat(left)
        right_time = datetime.fromisoformat(right)
    except ValueError:
        return False
    return abs((left_time - right_time).total_seconds()) <= 0.05


def _tool_name(span: dict) -> str | None:
    attrs = span.get("attributes") if isinstance(span.get("attributes"), dict) else {}
    tool_name = attrs.get("gen_ai.tool.name") or span.get("tool_name")
    observation_type = str(
        attrs.get("observation_type") or span.get("observation_type") or span.get("type") or ""
    ).lower()
    if not tool_name and "tool" in observation_type:
        tool_name = span.get("name")
    return str(tool_name) if tool_name else None


def _tool_call_id(span: dict) -> str | None:
    attrs = span.get("attributes") if isinstance(span.get("attributes"), dict) else {}
    value = attrs.get("gen_ai.tool.call.id")
    return str(value) if value else None


def _event(
    span_id: str, parent_span_id: str | None, timestamp: str | None, fields: dict
) -> dict:
    return {
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "timestamp": timestamp,
        **fields,
    }


def _messages(value: object, limitations: list[str], output: bool = False) -> list[dict]:
    parsed = _parse_json(value)
    if isinstance(parsed, dict):
        if isinstance(parsed.get("messages"), list):
            parsed = parsed["messages"]
        elif output and isinstance(parsed.get("tool_calls"), list):
            messages = _tool_call_messages(parsed["tool_calls"])
            content = parsed.get("content") or parsed.get("text") or parsed.get("refusal")
            if content is not None:
                messages.append(
                    {"kind": "assistant_message", "content": _text(content, limitations)}
                )
            return messages
        else:
            content = parsed.get("content") or parsed.get("text") or parsed.get("refusal")
            if content is None:
                return []
            parsed = content
    if isinstance(parsed, list):
        messages = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").lower()
            content = item.get("content") or item.get("text")
            # System/developer prompts and tool schemas describe configuration,
            # not observed behavior. Tool calls/results are represented by spans.
            if role == "assistant" and output and isinstance(item.get("tool_calls"), list):
                messages.extend(_tool_call_messages(item["tool_calls"]))
            if role not in {"user", "assistant", "context"} or content is None:
                continue
            messages.append(
                {
                    "kind": {
                        "user": "user_message",
                        "assistant": "assistant_message",
                        "context": "context_message",
                    }[role],
                    "content": _text(content, limitations),
                }
            )
        return messages
    if isinstance(parsed, str) and parsed:
        return [
            {
                "kind": "assistant_message" if output else "user_message",
                "content": _text(parsed, limitations),
            }
        ]
    return []


def _tool_call_messages(calls: list[object]) -> list[dict]:
    messages = []
    for call in calls:
        if not isinstance(call, dict):
            continue
        function = call.get("function")
        function = function if isinstance(function, dict) else {}
        name = call.get("name") or function.get("name")
        arguments = call.get("arguments")
        if arguments is None:
            arguments = function.get("arguments")
        messages.append(
            {
                "kind": "tool_call",
                "name": str(name or "unknown"),
                "call_id": call.get("id"),
                "payload": _payload(arguments),
            }
        )
    return messages


def _parse_json(value: object) -> object:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _payload(value: object) -> Any:
    return _parse_json(value)


def _text(value: object, limitations: list[str]) -> str:
    if isinstance(value, str):
        return _bounded_text(value, limitations)
    return _bounded_text(json.dumps(value, ensure_ascii=False), limitations)


def session_timestamp(spans: list[dict]) -> str:
    candidates = [str(span.get("end_time") or span.get("start_time") or "") for span in spans]
    candidates = [value for value in candidates if value]
    if candidates:
        return max(candidates).replace("+00:00", "Z")
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _bounded_text(text: str, limitations: list[str]) -> str:
    if len(text) > 20_000:
        limitations.append("Session message exceeded the 20000 character limit.")
    return text[:20_000]
