# LLM OTel GenAI sessions and traces

A client ingests multi-standard OpenTelemetry GenAI, OpenInference, OpenLLMetry, or Vercel AI spans without `sp.observation.type`, then reads session and trace details with canonical `span_type`, standard `service_name` and `span_kind`, clean JSON attributes, optional detail time bounds, and deduplicated usage-leaf token totals.

## Sub-features

- `multi-standard-classification` derives canonical `span_type` (`agent`, `generation`, `tool`, `embedding`, `chain`, `retriever`, `guardrail`, `evaluator`, `span`), `session_id`, `user_id`, `agent_name`, `model_name`, `model_provider`, and token counts across OTel GenAI (`gen_ai.*`, `user.id`, `session.id`), OpenInference (`openinference.span.kind`, `llm.*`), OpenLLMetry (`traceloop.*`), and Vercel AI (`ai.*`), keeping `sp.*` as fallback.
- `otel-service-and-kind` returns `service_name` (coalesced across promoted `service_name`, `resource_attributes['service.name']`, `attributes['service.name']`, and `app_id`), uppercase OTel `span_kind` (`INTERNAL`, `SERVER`, `CLIENT`, `PRODUCER`, `CONSUMER`), and `status_message` on `SpanSummary` and `SpanDetail`, plus `resource_attributes` on `SpanDetail`.
- `clean-json-attributes` strips internal `sp.json:` prefixes when serializing `SpanDetail.attributes` on `GET /v1/traces/{trace_id}` and `GET /v1/spans/{span_id}` so OTLP array and kvlist attributes return valid JSON strings.
- `optional-detail-bounds` serves `GET /v1/traces/{trace_id}` and `GET /v1/spans/{span_id}` with `200` when `from` and `to` query parameters are omitted, applying a finite bounded default timestamp window.
- `usage-leaf-token-rollup` sums `input_tokens`, `output_tokens`, and `total_tokens` only over usage-bearing spans that have no usage-bearing descendant in the same trace across `GET /v1/sessions/{session_id}`, `POST /v1/sessions/search`, `trace_summary_projection`, and `session_summary` compaction, preserving explicit `total_tokens` when `total_tokens > input_tokens + output_tokens` and excluding `recording` and `http_body` spans from `span_count` and `observation_count`.

## How to get to it (user POV)

- `POST http://127.0.0.1:18090/v1/traces`
- `GET http://127.0.0.1:18090/v1/sessions/{session_id}`
- `POST http://127.0.0.1:18090/v1/sessions/search`
- `GET http://127.0.0.1:18090/v1/traces/{trace_id}`
- `GET http://127.0.0.1:18090/v1/spans/{span_id}`

## Driving it with curl

Preconditions:

- The workspace from [Provision a workspace](./provision-workspace.md) exists and hot promotions (`docs/promotion/traces-query-hot-attrs.yaml`) are applied if testing promoted columns.
- `TOKEN` is the output of `scripts/mint_assertion.py --workspace-id "$WORKSPACE_ID" --secret "${SOFTPROBE_ASSERTION_HMAC_SECRET:-dev-assertion}"`.

- **Ingest pure OTel GenAI hierarchy.** `POST /v1/traces` with a recent timestamp (within the last hour so omitted `from` and `to` bounds cover it) containing:
  1. Root `agent` span (`spanId` `1111111111111111`, `kind` `1`, resource `service.name = "travel-agent"`, attributes `gen_ai.operation.name = "invoke_agent"`, `gen_ai.agent.name = "concierge"`, `gen_ai.conversation.id = "ses_verify_otel"`, `user.id = "usr_42"`, rollup `gen_ai.usage.input_tokens = 100`, `gen_ai.usage.output_tokens = 25`, `gen_ai.usage.total_tokens = 140`).
  2. Child `generation` span (`spanId` `2222222222222222`, `parentSpanId` `1111111111111111`, `kind` `3`, resource `service.name = "travel-agent"`, attributes `gen_ai.operation.name = "chat"`, `gen_ai.request.model = "gemini-2.5-flash"`, `gen_ai.provider.name = "gcp.vertex.agent"`, `gen_ai.usage.input_tokens = 100`, `gen_ai.usage.output_tokens = 25`, `gen_ai.usage.total_tokens = 140`, array `gen_ai.response.finish_reasons = ["tool_calls"]`).
  3. Child `tool` span (`spanId` `3333333333333333`, `parentSpanId` `1111111111111111`, `kind` `1`, attributes `gen_ai.operation.name = "execute_tool"`, `gen_ai.tool.name = "travel_search"`).
  4. Sidecar `http_body` span (`spanId` `4444444444444444`, `parentSpanId` `3333333333333333`, `name = "softprobe.http.body.capture"`, attributes `sp.observation.type = "http_body"`).
- **Verify session detail and usage-leaf rollup.** `GET /v1/sessions/ses_verify_otel` returns `200` with `user_id = "usr_42"`, `agent_name = "concierge"`, `input_tokens = 100`, `output_tokens = 25`, `total_tokens = 140` (not `200 / 50 / 280` and not overwritten to `125`), `span_count = 3` (excluding `http_body`), and span items classified as `agent`, `generation`, and `tool` with `service_name = "travel-agent"` and uppercase `span_kind`.
- **Verify omitted `from`/`to` and clean JSON attributes.** `GET /v1/traces/{trace_id}` and `GET /v1/spans/2222222222222222` (with no query string) both return `200`, `attributes["gen_ai.response.finish_reasons"]` equals `"[\"tool_calls\"]"` with no `"sp.json:"` prefix, and `resource_attributes["service.name"]` equals `"travel-agent"`.

## Gotchas

- Every `traces` and `logs` SQL query must still enforce finite lower and upper `timestamp` predicates (`scripts/check_sql_guardrails.py`). Omitted `from` and `to` on detail routes resolve to a bounded default window in the API layer, never an unbounded scan.
- `session_summary` SQL (`src/sql/session_summary/reduce_sql.rs`) may reference only promoted columns, never `attributes[...]` map lookups.
- Preserve `SpanDetail.app_id` on trace/span detail responses for existing callers while adding `service_name`, `span_kind`, `status_message`, and `resource_attributes`.
