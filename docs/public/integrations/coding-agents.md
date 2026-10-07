# Coding-agent integrations

Softprobe can ingest coding-agent sessions over **OTLP** using the Part A
tool-call contract (`agent` → `generation` → `tool`). See
[instrumentation.md](../instrumentation.md) and
[integrations-tool-calls.md](../integrations-tool-calls.md).

## Trace shape

```text
session (sp.session.id)
  └── agent turn                 # one per user prompt → final answer
        ├── generation           # LLM call
        │     └── tool*          # executions for that step
        ├── generation
        │     └── tool*
        └── event*               # retry / reasoning / compaction (optional)
```

Labels: `sp.user.id`, `sp.session.id`, `deployment.environment.name`,
`service.name` (agent product name).

## Integration matrix

| Agent | Softprobe approach | Status |
|---|---|---|
| **OpenCode** | `@softprobe/opencode-plugin` (hooks + `experimental.openTelemetry`) | Supported |
| GitHub Copilot | Point native GenAI OTLP at thelake `/v1/traces` | Planned |
| Claude Code | Stop/transcript hooks | Planned |
| OpenAI Codex | Plugin Stop hooks | Planned |
| Cursor | Extension / sidecar exporter | Planned |

## Auth

Softprobe OTLP uses a **bearer** token:

```http
Authorization: Bearer <publicKey>
```

Configure `SOFTPROBE_PUBLIC_KEY`, `SOFTPROBE_BASE_URL`, and
`SOFTPROBE_OTLP_ENDPOINT` (or the OpenCode config file).

## Content capture

Coding-agent integrations **always** record full payloads: prompts, completions,
reasoning text, and tool arguments/results. Metadata alone is not enough for
evaluation and agent improvement.

## OpenCode / spcode quick start

1. Enable the plugin in `opencode.json` / `opencode.jsonc` (spcode product builds
   inject this automatically):

```json
{
  "experimental": {
    "openTelemetry": true
  },
  "plugin": ["@softprobe/opencode-plugin@latest"]
}
```

2. Credentials — env (preferred) or file at
   `~/.config/spcode/opencode-softprobe.json` (spcode) or
   `~/.config/opencode/opencode-softprobe.json`:

```bash
export SOFTPROBE_PUBLIC_KEY="<bearer-token>"
export SOFTPROBE_BASE_URL="https://thelake.softprobe.ai"
```

```json
{
  "publicKey": "<bearer-token>",
  "baseUrl": "https://thelake.softprobe.ai",
  "environment": "production"
}
```

3. Restart spcode / OpenCode and run a session. Query in
   [explorer.softprobe.ai](https://explorer.softprobe.ai) or via API:

```bash
curl -sS -X POST https://thelake.softprobe.ai/v1/spans/search \
  -H "Authorization: Bearer $SOFTPROBE_PUBLIC_KEY" \
  -H "Content-Type: application/json" \
  -H "User-Agent: Mozilla/5.0" \
  -d '{"from":"2026-01-01T00:00:00Z","to":"2100-01-01T00:00:00Z","observation_types":["agent"],"limit":20}'
```

Use the OpenCode `sessionID` as `session_id` / `sp.session.id`.

Package details: [`@softprobe/opencode-plugin`](https://github.com/softprobe/softprobe-js/tree/main/packages/opencode-plugin).

Local verification (synthetic hooks → thelake):

```bash
make opencode-plugin-e2e
make opencode-plugin-e2e-inspect

# Against production thelake:
THELAKE_URL=https://thelake.softprobe.ai \
THELAKE_TOKEN=sp-llm-smoke-token \
  make opencode-plugin-e2e
```
