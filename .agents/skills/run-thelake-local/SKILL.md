---
name: run-thelake-local
description: Set up and run TheLake locally with its PostgreSQL DuckLake catalog, object storage, Explorer, and Lisa.
---

# Run TheLake locally

Use the repository docs as the source of truth and choose the smallest local
setup that matches the task.

## Standard developer stack

For Rust development and integration tests, follow `README.md`:

```bash
make doctor
make setup
make build
make run
```

`make setup` starts local PostgreSQL and MinIO with Docker Compose. PostgreSQL
is the DuckLake catalog; telemetry files use the configured `data_path`.
`make teardown` stops the Compose services. Check the Makefile before removing
volumes or local warehouse data; do not use destructive cleanup to fix a
startup problem.

For local Explorer and Lisa chat, use `docs/how-to/local-development.md` and
`make dev-up`. It provisions Lisa's own PGMQ-enabled Postgres service and starts
TheLake and Lisa as separate services. Lisa's Python server serves both the
Lisa UI and API on its own port; Explorer remains on TheLake.

When Lisa alone is needed, use `make dev-lisa-up`. It starts Lisa with its own
Postgres and provisioning dependency, without starting TheLake, MinIO,
DuckLake Postgres, or auth-mock. It leaves services that were already running
untouched.

When the user explicitly requests a full reset of local Lisa state, use
`make dev-reprovision-lisa`. It deletes only this Compose project's
`lisa-postgres-data` and `lisa-data` volumes, then runs `make dev-up`; it
leaves `.env` in place and preserves existing credentials, DuckLake, MinIO, and
TheLake telemetry. Normal environment initialization may add generated `.env`
settings or refresh defaults. The reset is destructive to Lisa's local
activities, policies, conversations, queued jobs, and agent workspace.

## Explorer development

Use `docs/how-to/explorer.md` for embedded UI setup, isolated/shared workspace
scope, or Vite development. `make test-explorer-ui` is the fastest real-stack
browser check. Use the online Gemini E2E only when explicitly requested; see
`run-lisa-evaluation-dogfood` and `run-thelake-tests`.

## Verify and troubleshoot

- Check `GET /health` and `GET /ready`; inspect the service logs and effective
  `CONFIG_FILE` before changing configuration.
- Compare the resolved DuckLake catalog, schema, and data path. A stale catalog
  attached with a different `data_path` can fail readiness.
- Keep Postgres, MinIO, and application data intact while diagnosing. Prefer a
  fresh local schema and data directory for an isolated reproduction.

For configuration keys and secret sources, read `docs/reference/config.md`.
