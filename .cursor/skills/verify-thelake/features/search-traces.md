# Search traces

A client sends one OTLP JSON trace, then searches that time window and sees the trace.

## Sub-features

- `ingest-json` accepts `POST /v1/traces` with the fixture body.
- `search-hit` returns that trace from `POST /v1/traces/search`.
- `search-other-workspace` does not return the trace for a second workspace.

## How to get to it (user POV)

- `POST http://127.0.0.1:18090/v1/traces`
- `POST http://127.0.0.1:18090/v1/traces/search`

## Driving it with curl

Preconditions:

- The workspace from [Provision a workspace](./provision-workspace.md) exists.
- `TOKEN` is the output of `scripts/mint_assertion.py --workspace-id "$WORKSPACE_ID" --secret "${SOFTPROBE_ASSERTION_HMAC_SECRET:-dev-assertion}"`.
- The fixture is `.cursor/skills/verify-thelake/scripts/fixtures/trace.json`.

- **Ingest.** Run `curl -sS -D /tmp/verify-thelake-evidence/ingest.headers -o /tmp/verify-thelake-evidence/ingest.json -X POST http://127.0.0.1:18090/v1/traces -H "X-Softprobe-Assertion: $TOKEN" -H 'content-type: application/json' --data-binary @.cursor/skills/verify-thelake/scripts/fixtures/trace.json`. Status is `200` and `ingested_count` is `1`.
- **Search.** Run `curl -sS -D /tmp/verify-thelake-evidence/search.headers -o /tmp/verify-thelake-evidence/search.json -X POST http://127.0.0.1:18090/v1/traces/search -H "X-Softprobe-Assertion: $TOKEN" -H 'content-type: application/json' -d '{"version":1,"scope":"traces","timeRange":{"from":"2026-05-03T10:00:00Z","to":"2026-05-03T11:00:00Z"},"limit":10}'`. Status is `200`. The body contains the trace from the fixture.
- **Other workspace.** Mint a token for a second UUID that was also provisioned. Repeat the search with that token. The body does not contain the first workspace's trace.

## Gotchas

- The assertion header wins over `Authorization` when both are present.
- An Agent assertion with `capabilities: ["ingest"]` can write traces and cannot search. This recipe uses a member assertion with no agent claims.
- Search without both `timeRange.from` and `timeRange.to` is rejected.
- `traceId` and `spanId` in the fixture are lowercase hex. OTLP JSON for this server rejects base64 ids.
- Shared scope mode serves both workspaces from one process pool. The other-workspace search is the proof that a reused query connection still hides the first workspace's rows. Set `workspace_scope_mode: shared` in `CONFIG_FILE` before launch when that is the proof you need.
