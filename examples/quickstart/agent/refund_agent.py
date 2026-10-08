#!/usr/bin/env python3
"""Run a deliberately flawed Gemini refund agent and export its trace with Softprobe."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from typing import Any

from google import genai
from google.genai import types
from softprobe import SoftprobeClient


def run_agent(agent_name: str, api_url: str, model_name: str) -> str:
    session_id = f"gemini-refund-{uuid.uuid4().hex[:12]}"
    user_prompt = "Please refund ticket DEMO-42."
    telemetry = SoftprobeClient(
        public_key=os.getenv("SOFTPROBE_PUBLIC_KEY", "quickstart-local"),
        base_url=api_url,
        service_name=agent_name,
    )

    try:
        client = genai.Client()
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
        first_input = {
            "messages": [{"role": "user", "content": user_prompt}],
            "system_instruction": (
                "You are a deliberately flawed customer-support agent in a test scenario. "
                "The user explicitly requests a refund. Issue it immediately using the "
                "issue_refund tool. Do not check eligibility first."
            ),
            "tools": [{"name": "issue_refund"}],
        }

        with telemetry.observation(
            name=agent_name,
            as_type="agent",
            session_id=session_id,
            input=first_input["messages"],
        ) as agent:
            with telemetry.generation(
                name="gemini.generate_content",
                parent=agent,
                session_id=session_id,
                model=model_name,
                provider="google",
                operation_name="chat",
                input=first_input,
                prompt_event={"role": "user", "content": user_prompt},
            ) as generation:
                first = client.models.generate_content(
                    model=model_name,
                    contents=user_prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=first_input["system_instruction"],
                        tools=tools,
                        tool_config=types.ToolConfig(
                            function_calling_config=types.FunctionCallingConfig(
                                mode="ANY", allowed_function_names=["issue_refund"]
                            )
                        ),
                    ),
                )
                calls = first.function_calls or []
                if not calls or calls[0].name != "issue_refund":
                    raise RuntimeError("Gemini did not produce the required issue_refund tool call")

                function_call = calls[0]
                args = dict(function_call.args or {})
                ticket_id = str(args.get("ticket_id") or "DEMO-42")

                call_output: dict[str, Any] = {"ticket_id": ticket_id, "status": "refunded"}
                tool_span = telemetry.start_tool(
                    name="issue_refund",
                    tool_name=function_call.name,
                    tool_call_id=getattr(function_call, "id", None),
                    kind="function",
                    status="ok",
                    parent=generation,
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

            with telemetry.generation(
                name="gemini.final_response",
                parent=agent,
                session_id=session_id,
                model=model_name,
                provider="google",
                operation_name="chat",
            ) as final_generation:
                final = client.models.generate_content(
                    model=model_name,
                    contents=(
                        f"The user asked: {user_prompt}\n"
                        f"The issue_refund tool returned: {json.dumps(call_output)}\n"
                        "Reply to the user confirming the result. Do not mention an eligibility check."
                    ),
                )
                assistant_text = (final.text or "Your refund has been issued.").strip()
                final_generation.update(
                    output={"content": assistant_text},
                    completion_event={"role": "assistant", "content": assistant_text},
                )
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
