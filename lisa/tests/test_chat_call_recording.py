"""Exercise installed LiteLLM/SDK hooks against a local, synthetic provider."""

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import litellm
import pytest
from test_llm_recording import context, records

from lisa.backends.llm_recording import install_call_logger
from lisa.llm_recording import recording_scope


@pytest.fixture
def provider():
    state = SimpleNamespace(requests=[], statuses=[])

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state.requests.append(body)
            status = state.statuses.pop(0) if state.statuses else 200
            if status != 200:
                result = {"error": {"message": "synthetic failure", "type": "server_error"}}
            elif self.path.endswith("/responses"):
                result = {
                    "id": "resp_test",
                    "object": "response",
                    "created_at": 1,
                    "model": "gpt-4o-mini",
                    "status": "completed",
                    "output": [
                        {
                            "type": "reasoning",
                            "id": "rs_test",
                            "summary": [{"type": "summary_text", "text": "hidden thought"}],
                        },
                        {
                            "type": "message",
                            "id": "msg_test",
                            "role": "assistant",
                            "status": "completed",
                            "content": [
                                {"type": "output_text", "text": "hello", "annotations": []}
                            ],
                        },
                    ],
                    "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
                }
            else:
                result = {
                    "id": "chat_test",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "gpt-4o-mini",
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "hello",
                                "reasoning_content": "hidden thought",
                            },
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
                }
            data = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_port}/v1"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def completed_records(ctx, count):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        paths = records(ctx)
        rows = [[json.loads(line) for line in p.read_text().splitlines()] for p in paths]
        if len(rows) == count and all(len(items) == 2 for items in rows):
            return rows
        time.sleep(0.02)
    pytest.fail("Provider terminal callbacks did not finish recording")


def test_sdk_completion_records_each_retry(provider, tmp_path, monkeypatch):
    from openhands.sdk import LLM
    from openhands.sdk.llm import Message, TextContent

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    provider.statuses = [500, 200]
    install_call_logger()
    llm = LLM(
        model="openai/gpt-4o-mini",
        api_key="synthetic-key",
        base_url=provider.url,
        num_retries=2,
        retry_min_wait=0,
        retry_max_wait=0,
        retry_multiplier=0,
    )
    with recording_scope(ctx, conversation_id="chat1"):
        llm.completion(messages=[Message(role="user", content=[TextContent(text="hello")])])
    rows = completed_records(ctx, 2)
    assert {items[1]["type"] for items in rows} == {"error", "response"}
    assert all(items[0]["correlation"]["conversation_id"] == "chat1" for items in rows)
    assert len(provider.requests) == 2
    assert all("synthetic-key" not in json.dumps(items) for items in rows)
    assert all("hidden thought" not in json.dumps(items) for items in rows)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_responses_hooks_record_schema_tools_and_scope(
    provider, tmp_path, monkeypatch, asynchronous
):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    install_call_logger()
    schema = {"format": {"type": "json_object"}}
    tools = [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}]
    kwargs = {
        "model": "openai/gpt-4o-mini",
        "api_key": "synthetic-key",
        "api_base": provider.url,
        "input": [{"role": "user", "content": "hello"}],
        "instructions": "system instruction",
        "text": schema,
        "tools": tools,
        "max_output_tokens": 100,
        "max_retries": 0,
    }

    async def run():
        with recording_scope(ctx, conversation_id="responses1"):
            await litellm.aresponses(**kwargs)
        # LiteLLM schedules async terminal logging on the owning event loop.
        for _ in range(100):
            if any(len(path.read_text().splitlines()) == 2 for path in records(ctx)):
                break
            await asyncio.sleep(0.02)

    if asynchronous:
        asyncio.run(run())
    else:
        with recording_scope(ctx, conversation_id="responses1"):
            litellm.responses(**kwargs)
    (rows,) = completed_records(ctx, 1)
    request = rows[0]["request"]
    assert request["messages"] == kwargs["input"]
    assert request["instructions"] == kwargs["instructions"]
    assert request["text"] == schema
    assert request["tools"] == tools
    assert request["max_output_tokens"] == 100
    assert "hidden thought" not in json.dumps(rows)
    assert rows[1]["response"]["usage"]["total_tokens"] == 12


def test_chat_tool_history_is_recorded_and_redacted(provider, tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    install_call_logger()
    messages = [
        {"role": "user", "content": "lookup"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": '{"password":"private"}'},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call1", "content": '{"result":false}'},
    ]
    tools = [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}]
    with recording_scope(ctx, conversation_id="tool-history"):
        litellm.completion(
            model="openai/gpt-4o-mini",
            api_key="synthetic-key",
            api_base=provider.url,
            messages=messages,
            tools=tools,
            max_retries=0,
        )
    (rows,) = completed_records(ctx, 1)
    recorded = rows[0]["request"]
    assert recorded["tools"] == tools
    assert recorded["messages"][1]["tool_calls"][0]["id"] == "call1"
    assert recorded["messages"][2]["content"] == '{"result":false}'
    assert "private" not in json.dumps(recorded)


def test_backend_worker_establishes_recording_scope(provider, tmp_path, monkeypatch):
    from lisa.backends import openhands
    from lisa.services.conversations import run_turn_without_releasing_lock_early

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    ctx = context(tmp_path)
    closed = []

    class Conversation:
        state = SimpleNamespace(events=[])

        def send_message(self, _text):
            pass

        def run(self):
            litellm.completion(
                model="openai/gpt-4o-mini",
                api_key="synthetic-key",
                api_base=provider.url,
                messages=[{"role": "user", "content": "hello"}],
                max_retries=0,
            )

        def close(self):
            closed.append(True)

    monkeypatch.setattr(openhands, "build_conversation", lambda *args, **kwargs: Conversation())
    monkeypatch.setattr(openhands, "conversation_content", lambda _: ([], []))
    result = asyncio.run(
        run_turn_without_releasing_lock_early(
            openhands.OpenHandsBackend().run_turn,
            conversation_id="worker-chat",
            workspace_id=ctx.workspace,
            directory=str(tmp_path),
            text="hello",
        )
    )
    assert result.messages[-1]["role"] == "assistant"
    assert "Open Activities" in result.messages[-1]["text"]
    (rows,) = completed_records(ctx, 1)
    assert rows[0]["correlation"]["conversation_id"] == "worker-chat"
    assert closed == [True]
