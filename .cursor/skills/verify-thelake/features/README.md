# thelake verification map

Read this index, then drive the matching feature file. The process must be one this run started.

## Baseline preconditions

- `make setup` has completed. Postgres answers as `ducklake` on `localhost:5432`. MinIO answers on port 9000.
- Launch with `.cursor/skills/verify-thelake/scripts/launch.sh`.
- Doctor reports `doctor ok` and `"status": "ok"`.
- Use a fresh workspace UUID per run. Do not reuse a schema name another run still owns.
- Never drive a thelake process this run did not start.

## Driving conventions

- Start from the baseline unless a feature names another precondition.
- Send JSON with `Content-Type: application/json`.
- Data routes use `X-Softprobe-Assertion`. Provisioning uses `Authorization: Bearer`.
- Save response bodies under `/tmp/verify-thelake-evidence/`.

## Proof and skip reporting

- Record the command, the status code, and the response body.
- A write is proved only after a later read returns the same trace.
- Report an unreachable path with the command and the missing precondition.

## Feature entry contract

Each feature file has an H1, one paragraph, then four H2 sections in order. `Sub-features`, `How to get to it (user POV)`, `Driving it with curl`, `Gotchas`.

## Features

- [Health](./health.md) covers `GET /health` and `GET /ready`.
- [Provision a workspace](./provision-workspace.md) covers `POST /v1/workspaces`.
- [Search traces](./search-traces.md) covers JSON OTLP ingest and `POST /v1/traces/search`.
- [LLM OTel GenAI sessions and traces](./llm-otel-genai-sessions.md) covers multi-standard GenAI classification without `sp.*`, `service_name` and `span_kind` projection, clean JSON attributes without `sp.json:`, optional `from` and `to` detail bounds, and usage-leaf token rollups.
