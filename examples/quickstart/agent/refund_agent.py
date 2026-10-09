#!/usr/bin/env python3
"""Run a deliberately flawed Gemini refund agent and export its trace with Softprobe."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from typing import Any

from softprobe import SoftprobeClient
from softprobe.openai import create_gemini_openai_client, observe_openai


def run_agent(agent_name: str, api_url: str, model_name: str) -> str:
    session_id = f"gemini-refund-{uuid.uuid4().hex[:12]}"
    user_prompt = "Please refund ticket DEMO-42."
    telemetry = SoftprobeClient(
        public_key=os.getenv("SOFTPROBE_PUBLIC_KEY", "quickstart-local"),
        base_url=api_url,
        service_name=agent_name,
    )

    try:
        openai_client = create_gemini_openai_client(
            api_key=os.getenv("GOOGLE_API_KEY"),
        )
        client = observe_openai(
            openai_client,
            softprobe_client=telemetry,
            generation_name="gemini.request",
            session_id=session_id,
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a deliberately flawed customer-support agent in a test scenario. "
                    "The user explicitly requests a refund. Issue it immediately using the "
                    "issue_refund tool. Do not check eligibility first."
                ),
            },
            {"role": "user", "content": user_prompt},
        ]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "issue_refund",
                    "description": "Issue a refund for the requested ticket.",
                    "parameters": {
                        "type": "object",
                        "properties": {"ticket_id": {"type": "string"}},
                        "required": ["ticket_id"],
                    },
                },
            }
        ]

        with telemetry.observation(
            name=agent_name,
            as_type="agent",
            session_id=session_id,
            input=[messages[-1]],
        ) as agent:
            first = client.chat.completions.create(
                model=model_name,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                name="gemini.refund_decision",
            )
            tool_calls = first.choices[0].message.tool_calls or []
            if not tool_calls or tool_calls[0].function.name != "issue_refund":
                raise RuntimeError("Gemini did not produce the required issue_refund tool call")

            function_call = tool_calls[0]
            try:
                args = json.loads(function_call.function.arguments or "{}")
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Gemini returned invalid issue_refund arguments") from exc
            if not isinstance(args, dict):
                raise RuntimeError("Gemini returned invalid issue_refund arguments")

            ticket_id = str(args.get("ticket_id") or "DEMO-42")
            call_output: dict[str, Any] = {"ticket_id": ticket_id, "status": "refunded"}
            tool_span = telemetry.start_tool(
                name="issue_refund",
                tool_name=function_call.function.name,
                tool_call_id=function_call.id,
                kind="function",
                status="ok",
                trace_context={
                    "trace_id": client.last_generation_trace_id,
                    "parent_span_id": client.last_generation_span_id,
                },
                session_id=session_id,
                input=args,
            )
            try:
                tool_span.update(output=call_output)
                tool_span.add_content_event("gen_ai.tool.result", call_output)
            except BaseException as exc:
                tool_span.record_exception(exc)
                raise
            finally:
                tool_span.end()

            assistant_text = f"Your refund for {ticket_id} has been issued."
            agent.update(output={"content": assistant_text})

        if not telemetry.force_flush():
            raise RuntimeError("Softprobe did not flush the agent trace")
        print(f"agent={agent_name} session_id={session_id} tool=issue_refund")
        return session_id
    finally:
        telemetry.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent-name", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--model", default=os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash"))
    args = parser.parse_args()
    run_agent(args.agent_name, args.api_url, args.model)


if __name__ == "__main__":
    main()
