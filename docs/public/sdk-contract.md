# Softprobe Instrumentation SDK Contract

This document defines the shared public contract for Softprobe Phase 2 SDKs:

- TypeScript: `@softprobe/tracing`
- Python: `softprobe`

Both packages must emit equivalent OTLP spans/events and score HTTP payloads.
Tenant identity is derived from the bearer token and must never appear as an SDK
payload field (`tenant_id` is not accepted on client APIs or score bodies).

## Configuration

Required:

- `publicKey` / `public_key`: Softprobe bearer token
- `baseUrl` / `base_url`: Softprobe runtime HTTP base (used for scores)

Optional:

- `otlpEndpoint` / `otlp_endpoint`: OTLP HTTP traces endpoint (defaults to
  `{baseUrl}/v1/traces`; protobuf payload)

- `serviceName` / `service_name` (default: `softprobe-app`)
- `serviceVersion` / `service_version`
- `environment`
- `release`
- `sessionId` / `session_id`
- `userId` / `user_id`
- `tags`: string array copied onto root-capable observations as `sp.tags`
- `redactKeys` / `redact_keys` (default: `password`, `api_key`, `authorization`,
  `secret`, `token`)
- `timeoutMs` / `timeout_ms`
- `headers`: extra HTTP headers for OTLP and score transport

## Provider integrations

Langfuse-style OpenAI wrappers (`softprobe.openai.observe_openai` /
`observeOpenAI`) auto-create `generation` observations for
`chat.completions.create`. See [OpenAI / Gemini integration](integrations-openai.md).

LangChain callback handlers and Vercel AI SDK telemetry are documented in
[LangChain](integrations-langchain.md) and
[Vercel AI SDK](integrations-vercel-ai.md).

Tool-call semantics (definitions vs calls vs executions, nesting, helpers)
are documented in [instrumentation.md](instrumentation.md) and
[integrations-tool-calls.md](integrations-tool-calls.md).

Start helpers accept optional `parent` / `parentSpanContext` /
`trace_context`, plus `metadata`, `version`, and `traceName` /
`trace_name`. Use `propagate_attributes` (Python) or `withAttributes` (TS)
to scope session/user/tags/metadata for nested work.

Tool helpers:

- `recordToolDefinitions` / `record_tool_definitions`
- `recordToolCalls` / `record_tool_calls`
- `startTool` / `start_tool` with `toolName` / `tool_name`, `toolCallId` /
  `tool_call_id`, `kind`, `index`, MCP fields, `status`
- `accumulateToolCallDeltas` / `accumulate_tool_call_deltas` for streaming
  fragments keyed by `index`

## Lifecycle

1. Construct `SoftprobeClient` with configuration.
2. Create observations with `startObservation` / `start_observation`, typed
   helpers (`startGeneration`, `startAgent`, ...), or scoped helpers.
3. Update attributes/events while open.
4. End observations explicitly or via scoped helpers / context managers.
5. Create scores with `createScore` / `create_score` against `/v1/llm/scores`.
6. Call `forceFlush` / `force_flush` before process exit checks.
7. Call `shutdown` once to flush and tear down exporters.

Historical / backdated spans (coding-agent plugins): start helpers accept
optional `startTime` (`Date` or epoch millis). `end` / `end_*` accept optional
`endTime` so completed transcripts keep accurate durations.

SDK construction and transport failures must surface as exceptions. Runtime
export failures after start should not crash user application code by default;
tests may inject failing transports to assert error logging behavior.

## Observation model

Canonical observation types (exact strings):

`span`, `event`, `generation`, `agent`, `tool`, `chain`, `retriever`,
`evaluator`, `embedding`, `guardrail`

Every started observation must set:

- span name
- `sp.observation.type`

Shared optional fields:

- `sp.session.id`, `gen_ai.conversation.id` (same value when session is known),
  `sp.user.id`, `sp.release`, `sp.tags`
- `sp.input`, `sp.output` (JSON string when capture is enabled)

`sp.session.id` is the product session for the whole conversation, including
nested sub-agents. SDKs MUST NOT stamp a child framework session as
`sp.session.id`. Nesting uses OpenTelemetry `parent_span_id` only.

Generation-specific fields:

- `gen_ai.operation.name`, `gen_ai.provider.name`
- `gen_ai.request.model`, `gen_ai.response.model`
- `gen_ai.request.temperature`, `gen_ai.request.max_tokens`
- `gen_ai.response.id`, `gen_ai.response.finish_reasons`
- `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`,
  `gen_ai.usage.total_tokens`
- `sp.cost.input`, `sp.cost.output`, `sp.cost.total`
- `sp.generation.completion_start_time`
- `sp.prompt.id`, `sp.prompt.name`, `sp.prompt.version`
- `sp.tool.available_names`, `sp.tool.available_count`
- `sp.tool.call_names`, `sp.tool.call_count`, `sp.tool.call_ids`

Tool-specific fields:

- `gen_ai.tool.name`, `gen_ai.tool.call.id`
- `sp.tool.kind`, `sp.tool.status`, `sp.tool.index`
- `sp.mcp.server`, `sp.mcp.tool`

## Content events

SDKs emit structured events with a JSON string `content` attribute:

- `gen_ai.content.prompt`
- `gen_ai.content.completion`
- `gen_ai.tool.message`
- `gen_ai.client.inference.operation.details`

Observation `sp.input` / `sp.output` are always recorded when provided.

## Privacy and redaction

Before serializing captured content, recursively redact object keys matching
`redactKeys` (case-insensitive). Replaced values use `[REDACTED]` by default.
See `contracts/fixtures/privacy-redaction.json`.

## Scores

`POST {baseUrl}/v1/llm/scores` with bearer auth.

Required body fields: `score_id`, `timestamp`, `name`, `data_type`, `source`.
Exactly one target among `span_id`, `trace_id`, or `session_id` must be usable
for the intended score scope (span scores may include both span and trace IDs).
Value fields must match `data_type`:

- numeric → `numeric_value`
- categorical / text → `string_value`
- boolean → `boolean_value`

Idempotency key is `score_id`. Replaying the same payload must be accepted by
the runtime as an idempotent create.

## Semantic parity fixtures

Language-neutral fixtures under `contracts/fixtures/` define expected normalized
spans and score requests. SDK unit/contract tests must normalize exported spans
into this shape and assert exact fixture equality for:

- nested parent/child relationships
- observation type coverage
- attribute keys/values
- content events
- score request shape

Validate fixtures with:

```bash
python3 scripts/validate_contracts.py
# or
pnpm validate:contracts
```

## Error behavior

- Invalid configuration: throw/raise at client construction.
- Invalid score payload: throw/raise before HTTP send.
- Double-end of an observation: no-op or ignored after first end.
- Exceptions inside scoped helpers: mark span status `ERROR`, record exception,
  rethrow/re-raise.
- Shutdown after shutdown: safe no-op.
