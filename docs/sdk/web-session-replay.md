# Web session replay

End-to-end browser session recording for Softprobe LLM sessions: the SPA
captures DOM mutations with rrweb, exports them to **thelake** as OTLP spans,
and the explorer **Web replay** tab plays them next to the conversation.

## Architecture

```text
softprobe-code SPA
  -> @softprobe/web-record
  -> POST {baseUrl}/v1/traces
  -> thelake-collector
  -> thelake DuckLake
  -> GET /v1/sessions/{session_id}/recording?from=&to=
  -> sp-llm explorer WebReplayPane
```

OpenCode chat `ses_*` identifiers or browser journey identifiers map to `session.id` and `sp.session.id` on recording and browser HTTP spans. `@softprobe/web-record` uses the official OpenTelemetry browser SDK internally. It propagates W3C `traceparent` to all same-origin and configured `apiOrigins` requests regardless of `httpBodyCapture.routes`, and injects W3C `baggage` with `sp.session.id` on same-origin `fetch` and `XMLHttpRequest` calls. Recording spans stay excluded from LLM session list and detail aggregates, conversation traces, `POST /v1/traces/search`, and default Tempo `/api/search` queries. Call `GET /v1/sessions/{session_id}/recording` for DOM replay.

Before OTLP spans reach **thelake**, `thelake-collector` handles external session correlation on `POST /v1/traces`:

1. Explicit `session.id`, `sp.session.id`, or `gen_ai.conversation.id` attributes stamp `sp.session.id` and register `(workspace_id, agent_id, trace_id)` in a bounded in-memory table with a 10-minute TTL.
2. W3C `baggage` in `http.request.header.baggage` or `http.request.headers` stamps `sp.session.id` directly when browser OTLP uploads fail.
3. Unresolved backend spans wait up to 2 seconds in `thelake-collector` for a matching sibling span, then pass through unmodified if no session arrives.

**thelake** itself stays stateless on ingest and read. When a non-recording span arrives without a session attribute, **thelake** writes `traces.session_id = ''` directly to DuckLake and returns `null` for `session_id` on JSON trace reads while omitting `sp.session.id` in Tempo responses. OTLP fact tables sort by `'session_id, timestamp, trace_id'` so assigned sessions remain in contiguous Parquet row groups while unassigned rows preserve tight `timestamp` zone maps.

The package embeds the official OpenTelemetry browser SDK, Fetch/XHR
instrumentation, and OTLP exporter. `RecordSdk.init()` configures those official
components internally; the host app imports only `RecordSdk` from
`@softprobe/web-record`. OTel owns browser HTTP spans, W3C context propagation,
batching, and export. Softprobe's extensions are route-scoped JSON body capture,
implemented with the instrumentation hooks, and the `sp.session.id`
compatibility alias alongside OTel's standard `session.id`. Captured bodies are
emitted on `softprobe.http.body.capture` child spans.

| Layer | Repo / package | Role |
| --- | --- | --- |
| Producer | [`@softprobe/web-record`](https://github.com/softprobe/softprobe-js/tree/main/packages/web-record) | rrweb capture and OpenTelemetry browser spans → OTLP |
| Host SPA | [`softprobe-code`](https://github.com/softprobe/softprobe-code) `web-record-boot.ts` | Boot after `ses_*` is in the URL |
| Store / query | [`thelake`](https://github.com/softprobe/thelake) | Ingest + `GET …/recording` |
| Player | [`sp-llm` Explorer](https://github.com/softprobe/sp-llm/tree/main/apps/explorer) | Conversation / Web replay tabs |

Merge order for the feature PRs: softprobe-js → thelake → softprobe-code → sp-llm.

## OTLP contract

| Field | Value |
| --- | --- |
| span name | `softprobe.web.recording` |
| `sp.observation.type` | `recording` |
| `sp.session.id` | OpenCode / LLM session id |
| `session.id` | same session id (OpenTelemetry browser SDK attribute) |
| `sp.recording.batch_index` | monotonic batch counter |
| event | `sp.recording.batch` |
| event attr `sp.recording.events` | JSON rrweb events (FullSnapshots may be `isCompressed`) |

Credentials and `{baseUrl}/v1/traces` come from `@softprobe/tracing/config`
(not reimplemented in hosts).

## Explorer

1. Run local thelake (`make e2e-up` from this repo) or point at a deployed runtime
   that includes the recording query API.
2. `pnpm --filter @softprobe/explorer dev` → http://127.0.0.1:5173
3. Connection (dev defaults): empty base URL (Vite `/v1` proxy → `:8091`) + token
   `e2e-token`. Hosted smoke defaults are rewritten in DEV when still set to
   production thelake.
4. Open a session → **Web replay**. Empty sessions show **No web recording**.

The player scales to the pane width (and height). Recording query responses may
set `truncated: true` when the batch limit (default 50, max 200) is hit — narrow
`from`/`to` for long sessions.

## Local verification

```bash
# SDK
cd softprobe-js && pnpm --filter @softprobe/web-record test && pnpm --filter @softprobe/web-record build

# thelake (needs libduckdb on LIBRARY_PATH / LD_LIBRARY_PATH)
cd thelake
cargo test --lib api::llm::query::tests::recording
cargo test --test tests session_recording_query -- --test-threads=1

# explorer
cd sp-llm/apps/explorer && npm test && npm run typecheck

# stack
cd sp-llm && make e2e-up
# softprobe-code: sibling softprobe-js required until @softprobe/web-record is published
# export INGESTION_KEY=e2e-token OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:8091/v1/traces
```

### Browser CORS

SPA exports use `Authorization: Bearer …`, which triggers a CORS OPTIONS
preflight **without** that header. thelake skips auth for `OPTIONS /v1/*` and
keeps CORS outermost so browser ingest works. Explorer still proxies `/v1` in
dev and strips `Origin` for hosted thelake quirks.

## Known limitations

- softprobe-code Vite aliases `@softprobe/web-record` to a **sibling**
  `softprobe-js` checkout until the package is published and declared in
  `packages/app/package.json`.
- Recording query hard-caps at 200 batches; UI surfaces empty / truncated data
  but does not paginate yet.
- Browser HTTP trace context uses W3C `traceparent` / `tracestate` propagation
  for same-origin requests (`window.location.origin`), any origin declared in
  `httpBodyCapture.routes`, and any optional `apiOrigins` override. Same-origin
  requests also carry `sp.session.id` in W3C `baggage` without triggering
  cross-origin CORS preflights; the SDK does not inject an `x-sp-session-id`
  header.

## Related docs

- thelake [instrumentation guide — web session recording](../how-to/instrumentation.md#web-session-recording-rrweb)
- [`@softprobe/web-record` README](https://github.com/softprobe/softprobe-js/blob/main/packages/web-record/README.md)
- [Explorer app README](https://github.com/softprobe/sp-llm/blob/main/apps/explorer/README.md)
