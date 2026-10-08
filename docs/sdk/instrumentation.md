# Instrumentation contract

## Scope

Softprobe SDKs produce standard OpenTelemetry spans. This document defines the
additional attributes and events needed for LLM observability.

The public SDK API, lifecycle, privacy controls, and fixture parity rules are
defined in [sdk-contract.md](sdk-contract.md). Machine-readable schemas and
expected fixtures live under [`contracts/`](../../contracts/).

Use stable OpenTelemetry semantic conventions where available. Softprobe keys
fill product-specific gaps and must use the `sp.` namespace.

## Resource attributes

| Attribute | Requirement | Description |
|---|---|---|
| `service.name` | required | Application or component name |
| `service.version` | recommended | Release/version |
| `deployment.environment.name` | recommended | `production`, `staging`, etc. |
| `service.instance.id` | optional | Runtime instance |
| `telemetry.sdk.name` | automatic | OpenTelemetry SDK name |
| `telemetry.sdk.version` | automatic | OpenTelemetry SDK version |

## Observation attributes

| Attribute | Type | Description |
|---|---|---|
| `sp.observation.type` | string | Observation type from the canonical enum |
| `sp.session.id` | string | Product session (primary conversation); constant for nested sub-agents |
| `sp.user.id` | string | Application user identifier |
| `sp.release` | string | Release override |
| `sp.tags` | string[] | Searchable labels |
| `sp.input` | JSON string | Non-LLM structured input when event encoding is unsuitable |
| `sp.output` | JSON string | Non-LLM structured output |

When `sp.session.id` is set, also set `gen_ai.conversation.id` to the same
value. Nested sub-agent work keeps that product session id and links via
`parent_span_id` (OTel), not a second session id.

## Generative AI attributes

Use the OpenTelemetry `gen_ai.*` keys:

| Attribute | Type |
|---|---|
| `gen_ai.operation.name` | string |
| `gen_ai.provider.name` | string |
| `gen_ai.request.model` | string |
| `gen_ai.response.model` | string |
| `gen_ai.request.temperature` | double |
| `gen_ai.request.max_tokens` | int |
| `gen_ai.response.id` | string |
| `gen_ai.response.finish_reasons` | string[] |
| `gen_ai.usage.input_tokens` | int |
| `gen_ai.usage.output_tokens` | int |
| `gen_ai.usage.total_tokens` | int |

Softprobe enrichment keys:

| Attribute | Type | Description |
|---|---|---|
| `sp.cost.input` | double | Input cost in configured project currency |
| `sp.cost.output` | double | Output cost |
| `sp.cost.total` | double | Total cost |
| `sp.generation.completion_start_time` | RFC3339 string | First-token time |
| `sp.prompt.id` | string | Managed/external prompt identifier |
| `sp.prompt.name` | string | Prompt name |
| `sp.prompt.version` | int | Prompt version |

## Tool-call roles (Part A)

Coding agents and tool-using chat flows distinguish three layers:

1. **Available tools** — definitions offered to the model on a `generation`
2. **Requested tool calls** — what the model chose to invoke (`tool_calls`)
3. **Tool executions** — one `tool` observation per invocation with arguments/results

Canonical nesting for multi-step agents:

```text
agent
  └── generation          # may list available tools + requested call ids
        └── tool*         # one child per execution (correlate by tool_call_id)
```

Parallel tool calls become sibling `tool` spans under the same generation,
optionally ordered with `sp.tool.index`.

### Generation attributes (model requested tools)

| Attribute | Type | Meaning |
|---|---|---|
| `sp.tool.available_names` | string[] | Names of tools offered in this request |
| `sp.tool.available_count` | int | Count of available tools |
| `sp.tool.call_names` | string[] | Names the model chose to call |
| `sp.tool.call_count` | int | Count of requested invocations |
| `sp.tool.call_ids` | string[] | Provider tool-call IDs when present |

Full schemas and argument objects stay in content events / `sp.input` /
`sp.output`, not promoted columns.

### Tool span attributes (execution)

| Attribute | Type | Meaning |
|---|---|---|
| `gen_ai.tool.name` | string | Canonical tool name |
| `gen_ai.tool.call.id` | string | Correlates to generation `sp.tool.call_ids` |
| `sp.tool.kind` | string | `function` \| `mcp` \| `shell` \| `file` \| `hook` \| `other` |
| `sp.tool.status` | string | `ok` \| `error` \| `cancelled` |
| `sp.tool.index` | int | Optional parallel-call order |
| `sp.mcp.server` | string | Optional MCP server id |
| `sp.mcp.tool` | string | Optional MCP tool name if distinct |

### Content events (when capture enabled)

- `gen_ai.content.prompt` / `gen_ai.content.completion` — may include `tools` /
  `tool_calls` in structured JSON
- `gen_ai.tool.message` — `{ role, name, tool_call_id, content }` for results

### Normalization

Flatten provider formats to Softprobe shapes at SDK time:

- Definitions: `{ name, description?, parameters? }`
- Calls: `{ id?, name, arguments }` (`arguments` may be a JSON string or object)

Streaming tool-call deltas should be buffered by `index` until the generation
ends (see `accumulateToolCallDeltas` / `accumulate_tool_call_deltas`). OpenAI
wrappers today instrument non-streaming `chat.completions.create`; the
accumulator is shared for future streaming.

### App-owned tool execution (OpenAI)

OpenAI wrappers record available tools and requested calls on the generation.
They do **not** invent execution spans. Applications wrap tool runs:

```python
with client.start_tool(
    name=call["name"],
    tool_name=call["name"],
    tool_call_id=call["id"],
    parent=generation,
) as tool:
    result = run_tool(call)
    tool.update(output=result)
```

Framework integrations (LangChain callbacks) create `tool` spans automatically.

Promotion of `sp.tool.*` filter columns into DuckLake is a thelake follow-up
after this contract stabilizes (Issue #7 A.4).

## Content events

Content is recorded as span events to preserve structured messages and avoid
inventing high-cardinality columns:

- `gen_ai.client.inference.operation.details`
- `gen_ai.content.prompt`
- `gen_ai.content.completion`
- `gen_ai.tool.message`

Event attributes should contain JSON-encoded structured messages. Sensitive
keys are redacted before serialization (see redaction below).

## Status and errors

- Set normal OpenTelemetry span status.
- Record exceptions using standard exception events.
- Use `status.error` only for operational failure, not for a low-quality model
  answer. Quality belongs in a score.
- Tool execution failures set span status `ERROR` and `sp.tool.status=error`.

## SDK API shape

Both languages expose equivalent concepts:

```text
client.start_observation(name, type, attributes)
client.start_generation(name, model, input, model_parameters)
client.start_tool(name, tool_name=, tool_call_id=, kind=, ...)
record_tool_definitions(observation, tools)
record_tool_calls(observation, tool_calls)
observation.update(attributes)
observation.end(output, usage, cost)
client.score(name, value, trace_id/span_id/session_id)
client.flush()
client.shutdown()
```

The API wraps the active OpenTelemetry context. Parent/child relationships use
standard trace context, not a proprietary hierarchy.

## Cardinality and privacy

- IDs may be high cardinality and remain queryable.
- Never promote raw prompt/completion content into columns.
- Do not record secrets, authorization headers, or credentials.
- SDKs always capture observation payloads and content events; they must redact
  sensitive keys (and may accept a redaction callback).
- Multimodal binary values are stored externally; events contain references
  and media metadata.
