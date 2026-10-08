# Tool-call instrumentation

Softprobe separates **tool definitions**, **requested tool calls**, and
**tool executions**. See [instrumentation.md](instrumentation.md) for the
attribute dictionary.

## OpenAI / Gemini (`observe_openai` / `observeOpenAI`)

Non-streaming `chat.completions.create` records:

- Request `tools` → `sp.tool.available_names` / `available_count`
- Response `message.tool_calls` → `sp.tool.call_*`
- Normalized calls in `sp.output` / completion events when capture is on

Execution spans are **app-owned**:

```python
from softprobe import SoftprobeClient
from softprobe.openai import observe_openai
from softprobe.tools import record_tool_calls  # also applied by the wrapper

# after observe_openai returns a response with tool_calls:
with client.start_tool(
    name=call["name"],
    tool_name=call["name"],
    tool_call_id=call["id"],
    kind="function",
    parent=generation,  # or ambient parent from withGeneration
    input=call.get("arguments"),
) as tool:
    result = execute(call)
    tool.update(output=result)
    # optional: tool.add_content_event("gen_ai.tool.message", {...})
```

TypeScript mirrors this with `startTool({ toolName, toolCallId, ... })`.

## LangChain

`CallbackHandler` maps tool callbacks to `tool` observations and sets
`gen_ai.tool.name`. `gen_ai.tool.call.id` is set only when a provider id is
available (`tool_call_id` in callback metadata); it is never fabricated from
the LangChain run id, so it always correlates with the parent generation's
`sp.tool.call_ids`. Generation ends that include AIMessage `tool_calls` also
set `sp.tool.call_*`.

## Vercel AI SDK

`@softprobe/vercel-ai-sdk` enriches tool spans with `gen_ai.tool.call.id`
and `gen_ai.tool.name` from AI SDK telemetry context.

## Verification

In this repository, validate language-neutral fixtures with:

```bash
make contracts-test
```

Provider / tool-call live E2E targets live in Softprobe SDK repositories, not
in thelake's Makefile.
