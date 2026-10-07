# Web session replay

End-to-end browser session recording for Softprobe LLM sessions: the SPA
captures DOM mutations with rrweb, exports them to **thelake** as OTLP spans,
and the explorer **Web replay** tab plays them next to the conversation.

## Architecture

```text
softprobe-code SPA
  → @softprobe/web-record (rrweb)
  → POST {baseUrl}/v1/traces   (OTLP JSON, Bearer publicKey)
  → thelake DuckLake
  → GET /v1/sessions/{session_id}/recording?from=&to=
  → sp-llm explorer WebReplayPane (rrweb-player)
```

**Correlation:** OpenCode chat `ses_*` id → `sp.session.id` on recording spans
(same attribute as LLM telemetry). Recording spans are **excluded** from LLM
session list/detail aggregates and conversation traces; use
`GET …/sessions/{id}/recording` for replay.

| Layer | Repo / package | Role |
| --- | --- | --- |
| Producer | [`@softprobe/web-record`](https://github.com/softprobe/softprobe-js/tree/main/packages/web-record) | rrweb capture → OTLP |
| Host SPA | [`softprobe-code`](https://github.com/softprobe/softprobe-code) `web-record-boot.ts` | Boot after `ses_*` is in the URL |
| Store / query | [`thelake`](https://github.com/softprobe/thelake) | Ingest + `GET …/recording` |
| Player | [`@softprobe/explorer`](../apps/explorer) | Conversation / Web replay tabs |

Merge order for the feature PRs: softprobe-js → thelake → softprobe-code → sp-llm.

## OTLP contract

| Field | Value |
| --- | --- |
| span name | `softprobe.web.recording` |
| `sp.observation.type` | `recording` |
| `sp.session.id` | OpenCode / LLM session id |
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
# export SOFTPROBE_BASE_URL=http://127.0.0.1:8091 SOFTPROBE_PUBLIC_KEY=e2e-token
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
- HTTP `x-sp-session-id` injection from the web-record interceptor is
  **same-origin only** (avoids leaking session ids to third-party CDNs).

## Related docs

- thelake [instrumentation guide — web session recording](https://github.com/softprobe/thelake/blob/main/docs/instrumentation_guide.md#web-session-recording-rrweb)
- [`@softprobe/web-record` README](https://github.com/softprobe/softprobe-js/blob/main/packages/web-record/README.md)
- Explorer [app README](../apps/explorer/README.md)
