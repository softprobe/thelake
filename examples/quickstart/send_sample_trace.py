#!/usr/bin/env python3
"""Send a synthetic refund-agent trace to a local theLake instance."""

from __future__ import annotations

import json
import ipaddress
import os
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


AGENT_NAME = "quickstart-refund-agent"


def validate_local_api_url(api_url: str) -> str:
    """Reject destinations outside loopback so demo data stays on this machine."""
    parsed = urlsplit(api_url)
    hostname = (parsed.hostname or "").lower().rstrip(".")
    try:
        is_loopback = hostname == "localhost" or ipaddress.ip_address(hostname).is_loopback
        _ = parsed.port
    except ValueError:
        is_loopback = hostname == "localhost"
        if parsed.port is not None:
            raise
    if parsed.scheme not in {"http", "https"} or not is_loopback or parsed.username or parsed.password:
        raise ValueError("THELAKE_API_URL must use HTTP(S) and point to a loopback address")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), parsed.query, ""))


def _attribute(key: str, value: str) -> dict[str, Any]:
    return {"key": key, "value": {"stringValue": value}}


def build_sample_trace(
    *, trace_id: str | None = None, session_id: str | None = None, now_ns: int | None = None
) -> tuple[dict[str, Any], str]:
    """Build a trace where the agent refunds before checking eligibility."""
    trace_id = trace_id or uuid.uuid4().hex
    session_id = session_id or f"quickstart-{uuid.uuid4().hex[:12]}"
    now_ns = now_ns if now_ns is not None else time.time_ns()
    start_ns = now_ns - 2_000_000_000
    root_span_id = uuid.uuid4().hex[:16]
    tool_span_id = uuid.uuid4().hex[:16]
    call_id = uuid.uuid4().hex[:12]

    root = {
        "traceId": trace_id,
        "spanId": root_span_id,
        "name": AGENT_NAME,
        "kind": 1,
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(start_ns + 1_000_000_000),
        "attributes": [
            _attribute("sp.agent.name", AGENT_NAME),
            _attribute("sp.observation.type", "agent"),
            _attribute("sp.session.id", session_id),
        ],
        "events": [
            {
                "name": "gen_ai.content.prompt",
                "timeUnixNano": str(start_ns + 10_000_000),
                "attributes": [
                    _attribute(
                        "content",
                        json.dumps(
                            [{"role": "user", "content": "Please refund my ticket."}]
                        ),
                    )
                ],
            },
            {
                "name": "gen_ai.content.completion",
                "timeUnixNano": str(start_ns + 900_000_000),
                "attributes": [
                    _attribute("content", "Your refund has been issued.")
                ],
            },
        ],
        "status": {"code": 1},
    }
    refund_tool = {
        "traceId": trace_id,
        "spanId": tool_span_id,
        "parentSpanId": root_span_id,
        "name": "issue_refund",
        "kind": 1,
        "startTimeUnixNano": str(start_ns + 200_000_000),
        "endTimeUnixNano": str(start_ns + 300_000_000),
        "attributes": [
            _attribute("sp.agent.name", AGENT_NAME),
            _attribute("sp.observation.type", "tool"),
            _attribute("gen_ai.tool.name", "issue_refund"),
            _attribute("gen_ai.tool.call.id", call_id),
            _attribute("sp.input", '{"ticket_id":"DEMO-42"}'),
            _attribute("sp.output", '{"status":"refunded"}'),
        ],
        "status": {"code": 1},
    }
    return (
        {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_attribute("service.name", AGENT_NAME)]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "thelake.quickstart"},
                            "spans": [root, refund_tool],
                        }
                    ],
                }
            ]
        },
        session_id,
    )


def main() -> None:
    api_url = os.getenv("THELAKE_API_URL")
    if not api_url:
        repo_root = Path(__file__).resolve().parents[2]
        url_file = repo_root / "warehouse/quickstart/api_url"
        try:
            with url_file.open(encoding="utf-8") as saved_url:
                api_url = saved_url.read().strip()
        except OSError:
            api_url = "http://127.0.0.1:8090"
    try:
        api_url = validate_local_api_url(api_url)
    except ValueError as error:
        raise SystemExit(f"Unsafe sample trace destination: {error}") from error
    body, session_id = build_sample_trace()
    request = urllib.request.Request(
        f"{api_url}/v1/traces",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response_body = response.read().decode()
    except Exception as error:
        raise SystemExit(f"Could not send the sample trace to {api_url}: {error}") from error

    print(f"Sent the refund demo trace. Session ID: {session_id}")
    print(f"In Explorer, open the {AGENT_NAME} session to see its evaluation result.")
    if response_body:
        print(response_body)


if __name__ == "__main__":
    main()
