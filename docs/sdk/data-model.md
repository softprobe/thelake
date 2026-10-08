# Telemetry data model

## Core concepts

### Observation

An observation is one unit of work represented by one OpenTelemetry span and
one row in thelake's `traces` DuckLake table.

Supported observation types:

- `span`
- `event`
- `generation`
- `agent`
- `tool`
- `chain`
- `retriever`
- `evaluator`
- `embedding`
- `guardrail`

The type refines the UI and query semantics; all types retain normal
OpenTelemetry span behavior.

### Trace

A trace is the set of observations sharing a `trace_id`. It is not a separate
mutable fact table. Its start, end, status, input, output, usage, and cost are
derived from constituent observations.

### Session

A session is the product unit for a user-facing conversation or job: primary
agent work and any nested sub-agents share one `session_id`. SDKs encode it as
`sp.session.id` and SHOULD mirror the same value on `gen_ai.conversation.id`.
thelake resolves the column from `sp.session.id` (or `sp_session_id`), then
`gen_ai.conversation.id`, then falls back to `trace_id` for basic correlation
(the fallback does not imply an explicit user session).

Nested agents MUST NOT introduce a second product session id. Nesting is the
OpenTelemetry span tree (`parent_span_id`), for example a sub-agent turn under
the parent’s dispatching `task` / tool span. Do not invent parallel Softprobe
“run id” attributes unless a concrete query or UI consumer ships in the same
change.

### Score

A score is an evaluation attached to a trace, observation, or session. Scores
are separate facts because evaluation commonly occurs after the observed work.

Examples:

- correctness: numeric `0.92`
- user feedback: categorical `thumbs_up`
- policy compliance: boolean `true`
- reviewer note: text `Answer cites the wrong source`

## Observation fact

The existing thelake span schema remains canonical. The LLM profile promotes
the following logical columns:

| Column | Type | Source |
|---|---|---|
| `observation_type` | string | `sp.observation.type` |
| `environment` | string | `deployment.environment.name` |
| `user_id` | string | `enduser.id` or `sp.user.id` |
| `model_provider` | string | `gen_ai.provider.name` |
| `model_name` | string | `gen_ai.request.model` |
| `response_model` | string | `gen_ai.response.model` |
| `operation_name` | string | `gen_ai.operation.name` |
| `input_tokens` | int64 | `gen_ai.usage.input_tokens` |
| `output_tokens` | int64 | `gen_ai.usage.output_tokens` |
| `total_tokens` | int64 | `gen_ai.usage.total_tokens` |
| `input_cost` | double | `sp.cost.input` |
| `output_cost` | double | `sp.cost.output` |
| `total_cost` | double | `sp.cost.total` |
| `completion_start_time` | timestamp | `sp.generation.completion_start_time` |
| `prompt_id` | string | `sp.prompt.id` |
| `prompt_name` | string | `sp.prompt.name` |
| `prompt_version` | int64 | `sp.prompt.version` |
| `release` | string | `service.version` or `sp.release` |

Unpromoted attributes remain in the attribute map. Prompt/completion content
uses OpenTelemetry events as defined by the instrumentation contract.

## Score fact

DuckLake table: `scores`.

| Column | Type | Required | Meaning |
|---|---|---:|---|
| `score_id` | string | yes | Client-generated idempotency key |
| `timestamp` | timestamp | yes | Evaluation time |
| `trace_id` | string | no | Target trace |
| `span_id` | string | no | Target observation |
| `session_id` | string | no | Target session |
| `name` | string | yes | Stable evaluation name |
| `data_type` | string | yes | `numeric`, `categorical`, `boolean`, or `text` |
| `numeric_value` | double | conditional | Numeric score |
| `string_value` | string | conditional | Categorical or text score |
| `boolean_value` | boolean | conditional | Boolean score |
| `source` | string | yes | `api`, `user`, `evaluator`, or `annotation` |
| `comment` | string | no | Human-readable explanation |
| `config_id` | string | no | Evaluator/configuration reference |
| `author_id` | string | no | User or service identity |
| `metadata` | map<string,string> | no | Additional dimensions |
| `record_date` | date | yes | Date locality/maintenance key |

Rules:

1. At least one of `trace_id`, `span_id`, or `session_id` is required.
2. Exactly one value column must match `data_type`.
3. `score_id` is unique within a tenant and makes retries idempotent.
4. Scores are immutable. Corrections create a new score and may reference the
   superseded score in metadata.
5. Tenant identity is never accepted in the score body.

## Query projections

All list and aggregate endpoints require an explicit time range except exact
identifier lookups for observation and trace detail. Pagination uses opaque
cursors ordered by `(timestamp DESC, id DESC)`. Tenant identity is never a
request parameter.

LLM fields (`observation_type`, model, tokens, cost, `user_id`) are derived
from span attribute maps (`gen_ai.*`, `sp.*`) at query time. Promotion makes
those fields typed filter columns but is not required for correctness.

### Span list (`POST /v1/spans/search`)

Returns identity, type, name, timing, status, model, token, cost, and selected
metadata fields. It does not return full attributes, events, or
prompt/completion payloads. Default limit 50, max 200.

### Span detail (`GET /v1/spans/{span_id}`)

Returns the full span, attributes, events, and attached scores for that span.

### Trace detail (`GET /v1/traces/{trace_id}`)

Returns a derived trace summary, paged observations (full payload), and scores
attached to the trace or any member span.

### Session summary (`GET /v1/sessions/{session_id}?from=&to=`)

Requires `from` and `to`. Returns bounded trace membership, users, aggregate
token/cost values, paged trace summaries, and scores attached to the session or
any member trace/span.
