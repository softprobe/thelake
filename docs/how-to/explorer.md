# Use thelake Explorer

For Lisa policy-evaluation terminology, use the shared
[Lisa evaluation terms](../architecture/lisa-terms.md). In particular, a
trajectory is derived from a session and has no separate ID.

Explorer is the self-hosted session and trace UI for thelake. Source lives in
[`packages/thelake-explorer`](../../packages/thelake-explorer/). `make build`
embeds the SPA into the binary; thelake serves it at `/explorer/`.

For a fast first run with Lisa, a live sample agent, and a detected evaluation
result in Explorer, see
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
scope (`ducklake.workspace_scope_mode`). In that mode, session/trace/score
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
single span/trace/session GET. Anyone
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

### Lisa

**Lisa** is a model-backed workspace QA agent running in the `lisa`
container. The current backend adapter uses the OpenHands SDK. Its reusable
tools are exposed over local stdio MCP and its workflows use standard
`SKILL.md` files, so another runtime can use the same tool and skill contract
without importing OpenHands code. Lisa supports persistent multi-turn chats,
workspace and agent policy records, bounded session-detail search,
explicitly approved policy rules, and scheduled policy evaluation. She does
not run shell commands or access a connected Git repository.

  - Explorer discloses beside the composer that messages and recent workspace
  telemetry go to the configured model provider. Sending a message proceeds
  without a separate checkbox. Lisa masks common credential patterns; general
  personal data is not redacted.
- Ask Lisa to propose a named policy section with `approved: yes/no`.
  Review it and send `Confirm policy rule: <exact heading>` to save the change.
  Approved rules are eligible immediately; there is no separate activation UI.
  Lisa's schedule reads policy and bounded session detail, builds a compact
  trajectory, then sends every approved candidate policy to OpenAI Decisions.
  Decisions classifies each policy as `suspected_violation`,
  `no_violation_found`, or `insufficient_evidence`; Lisa sends suspected and
  uncertain cases to Gemini for a categorical verdict. Triage negatives do not
  create pass scores. Lisa stores judge scores with the policy ID and criteria
  in TheLake.
  `LISA_QA_CRON` is a five-field UTC cron expression, default hourly at minute 0;
  the `Lisa scheduled QA` job does not run on startup.
- Conversations are stored in the runner's `lisa-data` volume, scoped by the
  authenticated workspace. One runtime service serves a deployment; this is
  not yet a separate VM per workspace or a multi-replica durable store. Delete
  a conversation from its header to remove its persisted OpenHands state.
- Lisa uses the `GOOGLE_API_KEY` or `GEMINI_API_KEY` and `LISA_MODEL` settings
  in the Lisa container. Explorer connects directly to Lisa using `LISA_PUBLIC_URL`. Lisa reads TheLake
  using `LISA_THELAKE_API_URL` and `LISA_THELAKE_TOKEN`.
- Lisa's evaluation flow uses `OPENAI_API_KEY` for Decisions triage, and
  `GOOGLE_API_KEY` or `GEMINI_API_KEY` plus optional `GEMINI_MODEL_NAME` for the
  judge. Lisa also needs a configured chat model and TheLake service credentials
  to read sessions and write scores.

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

Lisa runs the evaluator in her own MCP tool process. Compose passes the Gemini
key to Lisa; no evaluator service URL, token, or extra container is needed:

```bash
export GOOGLE_API_KEY=your-key
docker compose --profile lisa up --build
```

For a host-native `make run`, run Lisa with the same Gemini and TheLake
credentials. The OpenHands MCP subprocess receives only the credentials needed
to call Gemini and TheLake.

Then open Explorer → **Lisa** to ask questions about recent sessions and their
trajectories. Rule text and approval live in the Lisa Postgres policy table; missing approval means no. Lisa
selects matching sessions on her schedule and writes evaluation scores to
Sessions.

## Verify

```bash
make test-explorer-ui   # Playwright against a temporary thelake + embedded SPA
make test-explorer-online-e2e  # Real Gemini agent + Lisa's local judge, no service mocks
cd packages/thelake-explorer && npm test
```

The online E2E requires Docker, `GOOGLE_API_KEY` (or `GEMINI_API_KEY`), and
`OPENAI_API_KEY`. It makes real Gemini calls from the sample agent and judge,
and OpenAI calls for policy triage, so provider usage may be billed. Its refund
tool returns a local demo result; it does not connect
to an external ticketing or payment system.

## Related

- Auth and workspace binding: [`docs/compat/auth.md`](../compat/auth.md)
- HTTP API: [`docs/reference/openapi.yaml`](../reference/openapi.yaml)
- Session list summaries: [`docs/how-to/session-summaries.md`](session-summaries.md)
