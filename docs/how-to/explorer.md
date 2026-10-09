# Use thelake Explorer

Explorer is the self-hosted session and trace UI for thelake. Source lives in
[`packages/thelake-explorer`](../../packages/thelake-explorer/). `make build`
embeds the SPA into the binary; thelake serves it at `/explorer/`.

For a fast first run that creates a check in the guided wizard, runs a live
sample agent, and shows the evaluation result in Explorer, see
the [5-minute quickstart](../quickstart.md).

## Prerequisites

```bash
make setup    # Postgres (DuckLake catalog) + MinIO
make build    # embeds packages/thelake-explorer/embedded/ into the binary
```

## Open the UI (local)

Fastest proof the UI works end-to-end (ingest → reducer → session list →
trace detail):

```bash
make test-explorer-ui
```

That script builds embedded assets, starts thelake on port `18090` with
anonymous mode, a **shared** workspace scope, and a fresh metadata schema, then
runs Playwright against `/explorer/`.

### Manual local run

Anonymous mode lets the embedded SPA call `/v1` without a bearer. It only
**binds** requests to `THELAKE_DEFAULT_WORKSPACE_ID`. It does **not** provision
that workspace.

Stock [`config.yaml`](../../config.yaml) defaults to **isolated** workspace
scope (`ducklake.workspace_scope_mode`). In that mode, session/trace/evaluator
calls fail with `unknown scope` until you `POST /v1/workspaces` with
`SOFTPROBE_ADMIN_API_KEY` (admin provisioning is **not** on the anonymous
allowlist).

For a manual smoke that matches `make test-explorer-ui`, use **shared** scope
and a **fresh** metadata schema / data path (do not reuse a stale `softprobe`
catalog with a different `data_path`). Pick a free HTTP port if `:8090` is
already taken:

```bash
TMP=$(mktemp -d "${TMPDIR:-/tmp}/thelake-explorer-local.XXXXXX")
SCHEMA="explorer_local_$$"
PORT="${THELAKE_EXPLORER_PORT:-18090}"
mkdir -p "$TMP/warehouse"

cat > "$TMP/config.yaml" <<EOF
server:
  host: 127.0.0.1
  port: $PORT
  max_body_size: 104857600
query:
  max_connections: 1
ingest:
  flush_interval_seconds: 0
maintenance:
  enabled: false
  metadata_enabled: false
ducklake:
  metadata_path: "host=127.0.0.1 port=5432 dbname=ducklake user=ducklake password=ducklake"
  data_path: "$TMP/warehouse/"
  catalog_alias: softprobe
  metadata_schema: $SCHEMA
  workspace_scope_mode: shared
  extension_path: "./target/ducklake-extension/ducklake.duckdb_extension"
session_summary:
  reducer_interval_ms: 100
EOF

make ducklake-extension
export CONFIG_FILE="$TMP/config.yaml"
export SOFTPROBE_LOCAL_ANONYMOUS=1
export THELAKE_DEFAULT_WORKSPACE_ID=550e8400-e29b-41d4-a716-446655440000
export SOFTPROBE_LISTEN_ADDR="127.0.0.1:$PORT"
export SOFTPROBE_GRPC_DISABLE=1
make run
```

Open `http://127.0.0.1:$PORT/explorer/` (default
[http://127.0.0.1:18090/explorer/](http://127.0.0.1:18090/explorer/)).

Confirm readiness and the anonymous allowlist:

```bash
curl -fsS "http://127.0.0.1:${PORT:-18090}/ready"
curl -fsS "http://127.0.0.1:${PORT:-18090}/explorer/" | head
curl -fsS -X POST "http://127.0.0.1:${PORT:-18090}/v1/sessions/search" \
  -H 'Content-Type: application/json' \
  -d '{"from":"2020-01-01T00:00:00Z","to":"2099-01-01T00:00:00Z","order_by":"start_time","order":"desc","limit":5,"roots_only":true}'
# Non-allowlisted routes stay forbidden without a bearer:
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "http://127.0.0.1:${PORT:-18090}/v1/sql" \
  -H 'Content-Type: application/json' -d '{"sql":"select 1"}'   # expect 403
```

The embedded SPA calls `apiBasePath: "/v1"` with no auth headers
(`packages/thelake-explorer/src/standalone.tsx`). Anonymous mode allowlists OTLP
ingest, scores POST, span/session search, score-config GET,
single span/trace/session GET, and evaluator list/create/activate/deactivate. Anyone
who can reach the listener can exercise that allowlist — leave this mode off
when the listener is reachable by untrusted users.

Without anonymous mode, the SPA still loads, but API calls need a reverse proxy
or host app that injects Softprobe assertion / Bearer identity. See
[`docs/compat/auth.md`](../compat/auth.md).

Ingest traces (OTLP HTTP `/v1/traces` or gRPC `:4317`), wait for the session
summary reducer (often under a second with `reducer_interval_ms: 100`), then
refresh Sessions. Empty lists usually mean no data in the selected time range
for that workspace, or the summary has not caught up yet.

### Isolated mode (stock config.yaml)

If you keep the default isolated scope:

1. Set `SOFTPROBE_ADMIN_API_KEY` and start thelake with anonymous mode as in the
   root README.
2. Provision the same UUID you set in `THELAKE_DEFAULT_WORKSPACE_ID`:

```bash
curl -fsS -X POST http://127.0.0.1:8090/v1/workspaces \
  -H "Authorization: Bearer $SOFTPROBE_ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{
    \"workspaceId\": \"$THELAKE_DEFAULT_WORKSPACE_ID\",
    \"storageHints\": {
      \"ducklakeMetadataSchema\": \"explorer_ws\",
      \"ducklakeDataPath\": \"$(pwd)/warehouse/ducklake/data/explorer_ws/\"
    }
  }"
```

Use a new schema/path pair. Reusing an existing DuckLake catalog with a
mismatched `data_path` fails attach / readiness.

## What you can do in the UI

### Sessions

- List recent sessions (24h / 7d / 30d / 90d).
- Filter by agent name (API) and by session / model / user text (client-side).
- Open a session to load traces and spans; click a span for input/output,
  attributes, events, and scores.
- Record a human verdict (`correct` / `wrong` / `unsure`) as a categorical
  score named `human_verdict` on the focused span.

### Behavior-check wizard (labeled Chat in the UI)

The **Chat** view is a scripted setup wizard and results inbox, not a
conversational AI agent. Its assistant messages are fixed prompts; the composer
does not call an LLM, stream model output, or interpret free-form follow-ups.
It stores the entered criteria and target agent in browser state, then uses the
TheLake API to create and activate an evaluator. Once monitoring, it polls
session data for that evaluator's scores every 10 seconds. The online
evaluation runner separately judges matching completed traces after ingestion;
Gemini does not power the wizard.

- Create and activate a natural-language behavior check in Chat by describing
  the behavior and the exact agent name from its root trace.
- Follow multiple conversations in the sidebar. Chat history stays in memory by
  default; embedding applications can opt into browser persistence with a
  workspace-scoped `chatStorageKey` in Explorer config. Evaluator definitions
  and results remain stored by theLake.
- Pause or reactivate saved checks from the Chat sidebar. **Activate requires**
  `THELAKE_EVALUATION_RUNNER_URL` and `THELAKE_EVALUATION_RUNNER_TOKEN` to be
  set; otherwise activate returns HTTP 503
  (`online evaluation runner is not configured`). Creating a draft without
  activating does not need the runner.
- Activating sends captured prompts, responses, and tool data for matching
  agents to the configured Gemini provider via the evaluation runner (common
  credential patterns are redacted; this is not general PII scrubbing).

## Embed the React package

```tsx
import { ThelakeExplorer } from "@softprobe/thelake-explorer";
import "@softprobe/thelake-explorer/style.css";

<ThelakeExplorer
  config={{
    apiBasePath: "/api/thelake/v1", // or "/v1" behind your own proxy
    auth: {
      headers: () => ({
        Authorization: `Bearer ${token}`,
        // or: "X-Softprobe-Assertion": assertionJwt,
      }),
    },
  }}
/>
```

| Prop / config | Purpose |
|---------------|---------|
| `config.apiBasePath` | API root (no trailing slash required) |
| `config.auth.headers` | Optional per-request auth headers |
| `config.fetch` | Optional custom `fetch` |
| `className` | Optional CSS class on the root |
| `pageSize` | Session list page size (default 50) |
| `initialSessionId` | Optional session to open on mount |
| `onSessionOpen` | Optional callback when a session is selected |
| `onBack` | Optional back handler (otherwise clears selection) |

Package README: [`packages/thelake-explorer/README.md`](../../packages/thelake-explorer/README.md).

## Develop the SPA without rebuilding thelake

```bash
# Terminal 1 — thelake (shared-scope local config as above)
make run

# Terminal 2 — Vite SPA on :4173, proxies /v1 → thelake
cd packages/thelake-explorer
npm ci
THELAKE_API_PROXY=http://127.0.0.1:8090 npm run dev:spa
```

Open [http://127.0.0.1:4173/explorer/](http://127.0.0.1:4173/explorer/).
`vite.spa.config.ts` sets `base: "/explorer/"` and proxies `/v1` to
`THELAKE_API_PROXY` (default `http://127.0.0.1:18090` if unset).

Rebuild assets into the binary with:

```bash
make explorer-assets   # npm run build:embedded → packages/thelake-explorer/embedded/
make build             # embeds that folder via src/website.rs
```

On the Linux release builder, Explorer assets must already exist on the host
before entering the container (`IN_LINUX_BUILDER=1` does not run `npm`).

## Behavior evaluation

Compose starts the private runner on the Docker network (no host port published).
Point thelake at the service hostname, not `127.0.0.1`:

```bash
export THELAKE_EVALUATION_RUNNER_URL=http://evaluation-runner:8081/v1/evaluate
export THELAKE_EVALUATION_RUNNER_TOKEN=local-runner-token
export GOOGLE_API_KEY=your-key
docker compose --profile evaluation up --build
```

For a host-native `make run`, run the evaluation-runner on the host (see
[`evaluation-runner/README.md`](../../evaluation-runner/README.md)) and set
`THELAKE_EVALUATION_RUNNER_URL=http://127.0.0.1:8081/v1/evaluate` instead.

Then in Explorer → **Chat**, describe the check and enter the agent name on
its traces. Confirm the provider data notice and activate it. New matching
traces are evaluated and the result appears in that conversation; use
**Open trace in Sessions** to inspect the source evidence.

## Verify

```bash
make test-explorer-ui   # Playwright against a temporary thelake + embedded SPA
make test-explorer-online-e2e  # Real Gemini agent + runner + web chat, no service mocks
cd packages/thelake-explorer && npm test
```

The online E2E requires Docker and `GOOGLE_API_KEY` (or `GEMINI_API_KEY`). It
makes real Gemini calls from the sample agent and evaluator, so provider usage
may be billed. Its refund tool returns a local demo result; it does not connect
to an external ticketing or payment system.

## Related

- Auth and workspace binding: [`docs/compat/auth.md`](../compat/auth.md)
- HTTP API: [`docs/reference/openapi.yaml`](../reference/openapi.yaml)
- Session list summaries: [`docs/how-to/session-summaries.md`](session-summaries.md)
