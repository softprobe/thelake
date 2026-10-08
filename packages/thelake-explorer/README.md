# @softprobe/thelake-explorer

Shared trace/session UI for self-hosted theLake deployments.

**Usage guide (run, UI features, evaluation, Make targets):**
[`docs/how-to/explorer.md`](../../docs/how-to/explorer.md)

## Self-hosted SPA

`make build` / `make explorer-assets` produce `embedded/`, which thelake embeds
and serves at `/explorer/`. The standalone entry uses `apiBasePath: "/v1"` with
no auth headers.

Anonymous mode (`SOFTPROBE_LOCAL_ANONYMOUS=1` + `THELAKE_DEFAULT_WORKSPACE_ID`)
only binds auth; it does not provision an isolated workspace. For a working
local smoke, use shared scope with a fresh schema (see the how-to) or provision
via `POST /v1/workspaces` with `SOFTPROBE_ADMIN_API_KEY`.

```bash
make setup && make build
# then follow docs/how-to/explorer.md → Open the UI (local)
# → http://127.0.0.1:8090/explorer/
```

This package has no Cloudflare, Supabase, or hosted-service dependency.

## Embed as a React library

```tsx
import { ThelakeExplorer } from "@softprobe/thelake-explorer";
import "@softprobe/thelake-explorer/style.css";

<ThelakeExplorer
  config={{
    apiBasePath: "/api/thelake/v1",
    auth: {
      headers: () => ({ Authorization: `Bearer ${token}` }),
    },
  }}
/>
```

`ExplorerConfig`:

- `apiBasePath` — API root (for example `/v1` or `/api/thelake/v1`)
- `auth?.headers` — optional request headers (Bearer or `X-Softprobe-Assertion`)
- `fetch` — optional custom `fetch`

Optional UI props: `className`, `pageSize`, `initialSessionId`, `onSessionOpen`,
`onBack`.

The client calls thelake routes under that base: session search/detail, trace
detail, scores POST, and evaluator list/create/activate/deactivate.

## Local SPA development

```bash
npm ci
THELAKE_API_PROXY=http://127.0.0.1:8090 npm run dev:spa
# → http://127.0.0.1:4173/explorer/  (proxies /v1 → thelake)
```

```bash
npm run build:embedded   # → embedded/ for binary embed
npm run build            # library dist/ + embedded SPA
npm test
```

## UI surfaces

- **Sessions** — time range, agent/text filters, span tree, input/output
  inspector, human verdict scores (`human_verdict`).
- **Behavior checks** — create/activate natural-language evaluators for an
  agent. Activate requires `THELAKE_EVALUATION_RUNNER_URL` /
  `THELAKE_EVALUATION_RUNNER_TOKEN`; see
  [`evaluation-runner/README.md`](../../evaluation-runner/README.md).
