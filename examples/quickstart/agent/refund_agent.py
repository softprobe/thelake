#!/usr/bin/env python3
"""Run a deliberately flawed Gemini refund agent and export its real OTLP trace."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.request
import uuid
from typing import Any

from google import genai
from google.genai import types


def attribute(key: str, value: str | int) -> dict[str, Any]:
    item = {"intValue": str(value)} if isinstance(value, int) else {"stringValue": value}
    return {"key": key, "value": item}


def content_event(name: str, timestamp: int, content: str) -> dict[str, Any]:
    return {
        "name": name,
        "timeUnixNano": str(timestamp),
        "attributes": [attribute("content", content)],
    }


def run_agent(agent_name: str, api_url: str, model_name: str) -> str:
    client = genai.Client()
    trace_id = uuid.uuid4().hex
    session_id = f"gemini-refund-{uuid.uuid4().hex[:12]}"
    root_span_id, call_span_id, tool_span_id, answer_span_id = [uuid.uuid4().hex[:16] for _ in range(4)]
    call_id = uuid.uuid4().hex[:12]
    user_prompt = "Please refund ticket DEMO-42."

    issue_refund = types.FunctionDeclaration(
        name="issue_refund",
        description="Issue a refund for the requested ticket.",
        parameters_json_schema={
            "type": "object",
            "properties": {"ticket_id": {"type": "string"}},
            "required": ["ticket_id"],
        },
    )
    tools = [types.Tool(function_declarations=[issue_refund])]
    start_ns = time.time_ns()
    call_start_ns = time.time_ns()
    first = client.models.generate_content(
        model=model_name,
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=(
                "You are a deliberately flawed customer-support agent in a test scenario. "
                "The user explicitly requests a refund. Issue it immediately using the "
                "issue_refund tool. Do not check eligibility first."
            ),
            tools=tools,
            tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(
                    mode="ANY", allowed_function_names=["issue_refund"]
                )
            ),
        ),
    )
    call_end_ns = time.time_ns()
    calls = first.function_calls or []
    if not calls or calls[0].name != "issue_refund":
        raise RuntimeError("Gemini did not produce the required issue_refund tool call")
    args = dict(calls[0].args or {})
    ticket_id = str(args.get("ticket_id") or "DEMO-42")
    tool_start_ns = time.time_ns()
    call_output = {"ticket_id": ticket_id, "status": "refunded"}
    tool_end_ns = time.time_ns()

    answer_start_ns = time.time_ns()
    final = client.models.generate_content(
        model=model_name,
        contents=(
            f"The user asked: {user_prompt}\n"
            f"The issue_refund tool returned: {json.dumps(call_output)}\n"
            "Reply to the user confirming the result. Do not mention an eligibility check."
        ),
    )
    answer_end_ns = time.time_ns()
    assistant_text = (final.text or "Your refund has been issued.").strip()
    end_ns = time.time_ns()
    def span(span_id: str, name: str, start: int, end: int, attrs: list[dict[str, Any]], events: list[dict[str, Any]] | None = None, parent: str | None = None) -> dict[str, Any]:
        return {
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": parent or "",
            "name": name,
            "kind": 1,
            "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(max(end, start + 1)),
            "attributes": attrs,
            "events": events or [],
            "status": {"code": 1},
        }

    root = span(
        root_span_id,
        agent_name,
        start_ns,
        end_ns,
        [attribute("sp.agent.name", agent_name), attribute("sp.observation.type", "agent"), attribute("sp.session.id", session_id)],
    )
    call_span = span(
        call_span_id,
        "gemini.generate_content",
        call_start_ns,
        call_end_ns,
        [attribute("sp.agent.name", agent_name), attribute("sp.observation.type", "generation"), attribute("sp.session.id", session_id), attribute("gen_ai.request.model", model_name)],
        [content_event("gen_ai.content.prompt", call_start_ns + 1, user_prompt)],
        root_span_id,
    )
    tool_span = span(
        tool_span_id,
        "issue_refund",
        tool_start_ns,
        tool_end_ns,
        [attribute("sp.agent.name", agent_name), attribute("sp.observation.type", "tool"), attribute("sp.session.id", session_id), attribute("gen_ai.tool.name", calls[0].name), attribute("gen_ai.tool.call.id", call_id), attribute("sp.input", json.dumps(args)), attribute("sp.output", json.dumps(call_output))],
        [content_event("gen_ai.tool.result", tool_end_ns - 1, json.dumps(call_output))],
        root_span_id,
    )
    answer_span = span(
        answer_span_id,
        "gemini.final_response",
        answer_start_ns,
        answer_end_ns,
        [attribute("sp.agent.name", agent_name), attribute("sp.observation.type", "generation"), attribute("sp.session.id", session_id), attribute("gen_ai.request.model", model_name)],
        [content_event("gen_ai.content.completion", answer_end_ns - 1, assistant_text)],
        root_span_id,
    )
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": [attribute("service.name", agent_name)]},
            "scopeSpans": [{"scope": {"name": "thelake.quickstart.gemini-agent"}, "spans": [root, call_span, tool_span, answer_span]}],
        }],
    }
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}/v1/traces",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        if response.status >= 300:
            raise RuntimeError(f"trace ingest returned HTTP {response.status}")
    print(f"agent={agent_name} session_id={session_id} trace_id={trace_id} tool={calls[0].name}")
    return session_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent-name", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--model", default=os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash"))
    args = parser.parse_args()
    run_agent(args.agent_name, args.api_url, args.model)


if __name__ == "__main__":
    main()
