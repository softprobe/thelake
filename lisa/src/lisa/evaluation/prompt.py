"""Loss-aware, compact provider view of a stored trajectory."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from ..security import redact_evidence

TOOL_FORMAT_VERSION = "lisa-judge-tools/v4"
EVENT_BODY_LIMIT = 2_000
TRANSCRIPT_GUIDE = (
    "Trajectory lines are ordered events; bracketed identifiers are span IDs. "
    "Text uses backslash escapes (\\n newline, \\r carriage return, \\\\ backslash); "
    "structured payloads use compact JSON. Truncation markers mean text was omitted. "
    "Decisions use only visible evidence and may miss facts in omitted text."
)


COMPACT_TOOL_GUIDE = (
    "Events are one line: user:, assistant:, and context: contain escaped text. "
    "A tool line shows 'tool call: name arguments', then either 'result: data' or 'error, message'. "
    "Paired calls and results share a line; bracketed IDs identify their source spans. "
    "Tool bodies are compact JSON; clipping may leave partial JSON."
)


def escape_line(value: object) -> str:
    """Escape physical line separators and controls, preserving literal escapes."""
    return (
        json.dumps(str(value), ensure_ascii=False)[1:-1]
        .replace("\u0085", r"\u0085")
        .replace("\u2028", r"\u2028")
        .replace("\u2029", r"\u2029")
    )


def json_line(value: object, *, compact: bool = True) -> str:
    """Serialize one JSON value without characters that split physical lines."""
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":") if compact else None,
            allow_nan=False,
        )
        .replace("\u0085", r"\u0085")
        .replace("\u2028", r"\u2028")
        .replace("\u2029", r"\u2029")
    )


@dataclass(frozen=True)
class TrajectoryPrompt:
    text: str
    truncations: list[dict]
    span_id_map: dict[str, str]
    call_id_map: dict[str, str]

    @property
    def limitations(self) -> list[str]:
        return [
            "prompt_event_body_truncated: "
            f"sequence={item['sequence']} span_id={escape_line(item['span_id'])} "
            f"omitted_chars={item['omitted_chars']}"
            for item in self.truncations
        ]


def format_trajectory_for_llm(events: list[dict], *, short_ids: bool = True) -> TrajectoryPrompt:
    """Redact and format a copy; never change the persisted trajectory."""
    if not short_ids:
        return _format_selector_trajectory(events)
    return _format_compact_judge_trajectory(events)


def _format_selector_trajectory(source_events: list[dict]) -> TrajectoryPrompt:
    events = redact_evidence(source_events)
    lines, truncations = [], []
    call_aliases: dict[str, str] = {}
    span_aliases: dict[str, str] = {}
    tools_by_span: dict[str, list[dict]] = {}
    ordered_indices = sorted(range(len(events)), key=lambda i: events[i].get("sequence", i))
    for index in ordered_indices:
        event = source_events[index]
        if event["kind"] in {"tool_call", "tool_result"}:
            tools_by_span.setdefault(event.get("span_id", ""), []).append(event)
    for index in ordered_indices:
        event = events[index]
        identity = source_events[index]
        span = identity.get("span_id", "")
        span_aliases.setdefault(str(span), f"e{len(span_aliases) + 1}")
        related = tools_by_span.get(span, [])
        linked = (
            len(related) == 2
            and [item["kind"] for item in related] == ["tool_call", "tool_result"]
            and len({item.get("call_id") for item in related}) == 1
            and len({item.get("name") for item in related}) == 1
        )
        call_alias = None
        if identity.get("call_id") is not None and not linked:
            call_alias = call_aliases.setdefault(identity["call_id"], f"c{len(call_aliases) + 1}")
        header = f"[{escape_line(event.get('span_id', ''))}] "
        if event["kind"] in {"user_message", "assistant_message", "context_message"}:
            header += event["kind"].removesuffix("_message") + ":"
        else:
            header += event["kind"].removeprefix("tool_")
        if event.get("name") is not None:
            header += " " + escape_line(event["name"])
        if event.get("status") is not None:
            header += " " + escape_line(event["status"])
        if call_alias:
            header += " call_id=" + escape_line(event["call_id"])
        segments: list[tuple[str, bool]] = []
        if event.get("content") is not None:
            segments.append((event["content"], True))
        if "payload" in event:
            payload = json_line(event["payload"])
            segments.append(((" " if segments else "") + "payload=" + payload, False))
        size = sum(len(value) for value, _ in segments)
        remaining, body = EVENT_BODY_LIMIT, ""
        for value, plain in segments:
            fragment = value[:remaining]
            remaining -= len(fragment)
            body += (
                escape_line(fragment)
                if plain
                else fragment.replace("\u0085", r"\u0085")
                .replace("\u2028", r"\u2028")
                .replace("\u2029", r"\u2029")
            )
        if size > EVENT_BODY_LIMIT:
            omitted = size - EVENT_BODY_LIMIT
            body += f"…[truncated {omitted} chars]"
            truncations.append(
                {
                    "sequence": event.get("sequence", index),
                    "span_id": span,
                    "omitted_chars": omitted,
                }
            )
        lines.append(header + (" " + body if segments else ""))
    return TrajectoryPrompt(
        "\n".join(lines),
        truncations,
        {alias: span for span, alias in span_aliases.items()},
        {alias: call for call, alias in call_aliases.items()},
    )


def _format_compact_judge_trajectory(source_events: list[dict]) -> TrajectoryPrompt:
    events = redact_evidence(source_events)
    ordered = sorted(range(len(events)), key=lambda i: events[i].get("sequence", i))
    span_aliases: dict[str, str] = {}
    call_aliases: dict[str, str] = {}
    call_positions: dict[str, list[int]] = {}
    result_positions: dict[str, list[int]] = {}
    for position, index in enumerate(ordered):
        event = events[index]
        call_id = event.get("call_id")
        if call_id is not None and event["kind"] == "tool_call":
            call_positions.setdefault(str(call_id), []).append(position)
        elif call_id is not None and event["kind"] == "tool_result":
            result_positions.setdefault(str(call_id), []).append(position)

    span_calls: dict[str, list[int]] = {}
    span_results: dict[str, list[int]] = {}
    for position, index in enumerate(ordered):
        event = events[index]
        span_id = str(event.get("span_id", ""))
        if event["kind"] == "tool_call":
            span_calls.setdefault(span_id, []).append(position)
        elif event["kind"] == "tool_result":
            span_results.setdefault(span_id, []).append(position)

    def alias_for(index: int) -> str:
        span = str(source_events[index].get("span_id", ""))
        return span_aliases.setdefault(span, f"e{len(span_aliases) + 1}")

    def event_body(event: dict, index: int) -> str:
        segments: list[str] = []
        content = event.get("content")
        has_content = content is not None
        has_payload = "payload" in event
        if has_content and has_payload:
            segments.append("content=" + json_line(content, compact=False))
            segments.append(
                "payload="
                + json_line(event["payload"])
            )
        elif has_payload:
            segments.append(json_line(event["payload"]))
        elif has_content:
            segments.append(json_line(content, compact=False))
        serialized = " ".join(segments)
        if len(serialized) <= EVENT_BODY_LIMIT:
            return serialized
        omitted = len(serialized) - EVENT_BODY_LIMIT
        span = str(source_events[index].get("span_id", ""))
        truncations.append(
            {
                "sequence": event.get("sequence", index),
                "span_id": span,
                "omitted_chars": omitted,
            }
        )
        return serialized[:EVENT_BODY_LIMIT] + f"…[truncated {omitted} chars]"

    def is_error(event: dict) -> bool:
        return str(event.get("status", "")).strip().lower() in {
            "error", "failed", "failure", "status_code_error"
        }

    def tool_name(event: dict) -> str:
        name = str(event.get("name") or "unknown")
        return name if re.fullmatch(r"[A-Za-z0-9_./-]+", name) else json_line(name, compact=False)

    def error_clause(event: dict, index: int) -> str:
        clause = f"[{alias_for(index)}] error"
        body = event_body(event, index)
        if body:
            return clause + ", " + body
        return clause

    lines: list[str] = []
    truncations: list[dict] = []
    consumed: set[int] = set()
    for position, index in enumerate(ordered):
        if index in consumed:
            continue
        event = events[index]
        identity = source_events[index]
        kind = event["kind"]
        span_alias = alias_for(index)
        if kind in {"user_message", "assistant_message", "context_message"}:
            content = event.get("content", "")
            has_payload = "payload" in event
            payload_text = ""
            if has_payload:
                payload_text = json_line(event["payload"])
            payload_prefix = (" payload=" if content else "payload=") if has_payload else ""
            raw_parts = [(content, True)]
            if has_payload:
                raw_parts.append((payload_prefix + payload_text, False))
            raw_size = sum(len(value) for value, _ in raw_parts)
            remaining = EVENT_BODY_LIMIT
            body = ""
            for value, is_text in raw_parts:
                fragment = value[:remaining]
                remaining -= len(fragment)
                body += escape_line(fragment) if is_text else fragment
            if raw_size > EVENT_BODY_LIMIT:
                omitted = raw_size - EVENT_BODY_LIMIT
                truncations.append(
                    {
                        "sequence": event.get("sequence", index),
                        "span_id": identity.get("span_id", ""),
                        "omitted_chars": omitted,
                    }
                )
                body += f"…[truncated {omitted} chars]"
            role = kind.removesuffix("_message")
            lines.append(f"[{span_alias}] {role}: {body}")
            continue

        call_id = identity.get("call_id")
        call_key = str(call_id) if call_id is not None else None
        paired_index = None
        if kind == "tool_call" and call_key is not None:
            candidates = result_positions.get(call_key, [])
            if (
                len(call_positions.get(call_key, [])) == 1
                and len(candidates) == 1
                and candidates[0] == position + 1
                and events[ordered[candidates[0]]].get("name") == event.get("name")
            ):
                paired_index = ordered[candidates[0]]

        if kind == "tool_call":
            if call_key is None:
                span_id = str(identity.get("span_id", ""))
                candidates = span_results.get(span_id, [])
                if (
                    len(span_calls.get(span_id, [])) == 1
                    and len(candidates) == 1
                    and candidates[0] == position + 1
                    and events[ordered[candidates[0]]].get("name") == event.get("name")
                ):
                    paired_index = ordered[candidates[0]]
            call_line = f"[{span_alias}] tool call: {tool_name(event)}"
            if call_key is not None and paired_index is None:
                call_alias = call_aliases.setdefault(call_key, f"c{len(call_aliases) + 1}")
                call_line += f" call_id={call_alias}"
            call_body = event_body(event, index)
            if call_body:
                call_line += " " + call_body
            if paired_index is not None:
                result_event = events[paired_index]
                result_alias = alias_for(paired_index)
                if is_error(result_event):
                    result_clause = error_clause(result_event, paired_index)
                else:
                    result_body = event_body(result_event, paired_index)
                    result_clause = f"[{result_alias}] result: {result_body}"
                lines.append(call_line + "; " + result_clause)
                consumed.add(paired_index)
            else:
                lines.append(call_line)
        elif kind == "tool_result":
            unique_call = (
                call_key is not None
                and len(call_positions.get(call_key, [])) == 1
                and events[ordered[call_positions[call_key][0]]].get("name") == event.get("name")
            )
            if is_error(event):
                line = f"[{span_alias}] error"
                if not unique_call and event.get("name") is not None:
                    line += " " + tool_name(event)
                if call_key is not None:
                    call_alias = call_aliases.setdefault(call_key, f"c{len(call_aliases) + 1}")
                    line += f" call_id={call_alias}"
                clause = error_clause(event, index).split(" error", 1)[1]
                line += clause
            else:
                line = f"[{span_alias}] result:"
                if not unique_call and event.get("name") is not None:
                    line += " " + tool_name(event)
            if not is_error(event) and call_key is not None:
                call_alias = call_aliases.setdefault(call_key, f"c{len(call_aliases) + 1}")
                line += f" call_id={call_alias}"
            if not is_error(event):
                body = event_body(event, index)
                if body:
                    line += " " + body
            lines.append(line)
        else:
            label = alias_for(index)
            lines.append(f"[{label}] {kind}: {event_body(event, index)}")

    return TrajectoryPrompt(
        "\n".join(lines),
        truncations,
        {alias: span for span, alias in span_aliases.items()},
        {alias: call for call, alias in call_aliases.items()},
    )
