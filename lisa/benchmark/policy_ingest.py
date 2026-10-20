"""Build OTLP fixtures from the labeled policy-evaluation trajectories.

Offline harness only — see ``benchmark`` package docs. Do not relocate into
``lisa.evaluation``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

_BASE_TIME_NS = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp()) * 1_000_000_000
_NANOSECONDS_PER_SEQUENCE = 1_000_000


def _string_attribute(key: str, value: str) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def _json_attribute(key: str, value: object) -> dict:
    return _string_attribute(key, json.dumps(value, ensure_ascii=False))


def _time(sequence: int) -> str:
    # Keep every timestamp fractional so ISO-8601 sorting preserves sequence
    # around the second boundary (".001Z" must sort after "Z").
    return str(_BASE_TIME_NS + (sequence + 1) * _NANOSECONDS_PER_SEQUENCE)


def build_otlp_trace_export(
    trajectory: dict,
    *,
    session_id: str,
    agent_name: str,
    trace_id: str,
) -> dict:
    """Represent the dataset's compact events as ingestible OTLP spans.

    See ``docs/architecture/lisa-terms.md`` for dataset and runtime pair IDs.
    The conversion preserves user/tool/assistant semantics so Lisa's normal
    SessionService reconstructs the trajectory before evaluation. Dataset
    span IDs and trace IDs are retained when they are valid OTLP IDs. The
    evaluation runner preserves each dataset session ID and rejects a workspace
    that already contains it, preventing collisions with source telemetry.
    """
    events = sorted(trajectory["events"], key=lambda event: event["sequence"])
    if len({event["sequence"] for event in events}) != len(events):
        raise ValueError("Dataset trajectory event sequences must be unique")

    calls: dict[str, dict] = {}
    results: dict[str, dict] = {}
    for event in events:
        call_id = event.get("call_id")
        if event["kind"] == "tool_call":
            calls[str(call_id or event["sequence"])] = event
        elif event["kind"] == "tool_result":
            results[str(call_id or event["sequence"])] = event

    spans = []
    emitted_calls = set()
    for event in events:
        kind = event["kind"]
        sequence = int(event["sequence"])
        if kind in {"user_message", "assistant_message", "context_message"}:
            attributes = [
                _string_attribute("sp.session.id", session_id),
                _string_attribute("sp.observation.type", "agent"),
            ]
            if kind in {"user_message", "context_message"}:
                role = "user" if kind == "user_message" else "context"
                attributes.append(
                    _json_attribute("sp.input", [{"role": role, "content": event["content"]}])
                )
            else:
                attributes.append(_json_attribute("sp.output", {"content": event["content"]}))
            start = _time(sequence)
            end = str(int(start) + 1)
            spans.append(
                {
                    "traceId": trace_id,
                    "spanId": event["span_id"],
                    "name": agent_name,
                    "kind": 1,
                    "startTimeUnixNano": start,
                    "endTimeUnixNano": end,
                    "attributes": attributes,
                    "status": {"code": 1},
                }
            )
        elif kind == "tool_call":
            key = str(event.get("call_id") or sequence)
            if key in emitted_calls:
                continue
            emitted_calls.add(key)
            result = results.get(key)
            attributes = [
                _string_attribute("sp.session.id", session_id),
                _string_attribute("sp.observation.type", "tool"),
                _string_attribute("gen_ai.tool.name", str(event.get("name") or "tool")),
                _string_attribute("gen_ai.tool.call.id", str(event.get("call_id") or key)),
                _json_attribute("sp.input", event.get("payload", {})),
            ]
            if result is not None:
                attributes.extend(
                    [
                        _json_attribute("sp.output", result.get("payload", {})),
                    ]
                )
                if result.get("status") is not None:
                    attributes.append(_string_attribute("sp.tool.status", str(result["status"])))
            span = {
                "traceId": trace_id,
                "spanId": event["span_id"],
                "name": str(event.get("name") or "tool"),
                "kind": 1,
                "startTimeUnixNano": _time(sequence),
                "attributes": attributes,
            }
            if result is not None:
                span["endTimeUnixNano"] = _time(int(result["sequence"]))
                if result.get("status") is not None:
                    span["status"] = {
                        "code": 2 if result["status"] in {"error", "failed", "failure"} else 1
                    }
            spans.append(span)

    return {
        "resourceSpans": [
            {
                "resource": {"attributes": [_string_attribute("service.name", agent_name)]},
                "scopeSpans": [
                    {
                        "scope": {"name": "lisa.policy.evaluation.dataset"},
                        "spans": spans,
                    }
                ],
            }
        ]
    }
