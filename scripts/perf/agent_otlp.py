"""Build OTLP JSON ExportTraceServiceRequest bodies for AI-agent sessions."""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

# Softprobe / GenAI keys (keep in lockstep with src/models/attr_keys.rs)
SP_SESSION_ID = "sp.session.id"
SP_OBSERVATION_TYPE = "sp.observation.type"
SP_AGENT_NAME = "sp.agent.name"
SP_USER_ID = "sp.user.id"
SP_COST_TOTAL = "sp.cost.total"
GEN_AI_MODEL = "gen_ai.request.model"
GEN_AI_PROVIDER = "gen_ai.provider.name"
GEN_AI_IN_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_OUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_TOTAL_TOKENS = "gen_ai.usage.total_tokens"
TOOL_NAME = "gen_ai.tool.name"
SERVICE_NAME = "service.name"


def _sv(s: str) -> dict[str, Any]:
    return {"stringValue": s}


def _iv(n: int) -> dict[str, Any]:
    return {"intValue": str(n)}


def _dv(x: float) -> dict[str, Any]:
    return {"doubleValue": x}


def _attr(key: str, value: dict[str, Any]) -> dict[str, Any]:
    return {"key": key, "value": value}


def _hex_id(n_bytes: int) -> str:
    return os.urandom(n_bytes).hex()


def build_agent_session(
    *,
    session_id: str | None = None,
    traces_per_session: int = 3,
    spans_per_trace: int = 5,
    agent_name: str = "northwind-agent",
    user_id: str = "user-bench",
    model: str = "gpt-4o",
    now_ns: int | None = None,
) -> tuple[dict[str, Any], str, int]:
    """Return (OTLP JSON body, session_id, span_count).

    Hierarchy per trace:
      agent (root) → generation, tool, reasoning, optional extra spans
    """
    session_id = session_id or f"bench-sess-{uuid.uuid4().hex[:16]}"
    now_ns = now_ns if now_ns is not None else time.time_ns()
    all_spans: list[dict[str, Any]] = []
    span_count = 0

    for t in range(traces_per_session):
        trace_id = _hex_id(16)
        agent_span_id = _hex_id(8)
        base = now_ns - (traces_per_session - t) * 1_000_000_000

        def add_span(
            *,
            span_id: str,
            parent: str,
            name: str,
            obs: str,
            start_off_ms: int,
            dur_ms: int,
            extra: list[dict[str, Any]] | None = None,
        ) -> None:
            nonlocal span_count
            attrs = [
                _attr(SP_SESSION_ID, _sv(session_id)),
                _attr(SP_OBSERVATION_TYPE, _sv(obs)),
                _attr(SP_AGENT_NAME, _sv(agent_name)),
                _attr(SP_USER_ID, _sv(user_id)),
            ]
            if extra:
                attrs.extend(extra)
            all_spans.append(
                {
                    "traceId": trace_id,
                    "spanId": span_id,
                    "parentSpanId": parent,
                    "name": name,
                    "kind": 1,  # INTERNAL
                    "startTimeUnixNano": str(base + start_off_ms * 1_000_000),
                    "endTimeUnixNano": str(base + (start_off_ms + dur_ms) * 1_000_000),
                    "attributes": attrs,
                    "status": {"code": 1, "message": ""},
                }
            )
            span_count += 1

        add_span(
            span_id=agent_span_id,
            parent="",
            name=agent_name,
            obs="agent",
            start_off_ms=0,
            dur_ms=200 + spans_per_trace * 40,
        )

        # Fill remaining slots with generation / tool / reasoning pattern
        kinds = ["generation", "tool", "reasoning", "span"]
        for i in range(max(1, spans_per_trace - 1)):
            kind = kinds[i % len(kinds)]
            child_id = _hex_id(8)
            extra: list[dict[str, Any]] = []
            name = kind
            if kind == "generation":
                name = "chat.completions"
                tokens = 100 + (i * 10)
                extra = [
                    _attr(GEN_AI_MODEL, _sv(model)),
                    _attr(GEN_AI_PROVIDER, _sv("openai")),
                    _attr(GEN_AI_IN_TOKENS, _iv(tokens // 2)),
                    _attr(GEN_AI_OUT_TOKENS, _iv(tokens // 2)),
                    _attr(GEN_AI_TOTAL_TOKENS, _iv(tokens)),
                    _attr(SP_COST_TOTAL, _dv(0.001 * tokens)),
                ]
            elif kind == "tool":
                name = f"tool.search_{i}"
                extra = [_attr(TOOL_NAME, _sv(f"search_{i}"))]
            elif kind == "reasoning":
                name = "reasoning"
            add_span(
                span_id=child_id,
                parent=agent_span_id,
                name=name,
                obs=kind if kind != "span" else "span",
                start_off_ms=20 + i * 30,
                dur_ms=25 + i * 5,
                extra=extra,
            )

    body = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        _attr(SERVICE_NAME, _sv("llm-gateway")),
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "softprobe.llm"},
                        "spans": all_spans,
                    }
                ],
            }
        ]
    }
    return body, session_id, span_count
