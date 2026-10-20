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
`make dev-up`. It provisions Lisa's policy database on the existing local
Postgres service and starts TheLake and Lisa together. The local stack disables
scheduled QA by default; enabling it can incur provider charges.
Explorer is served at `/explorer/`; Lisa is a separate app at `/lisa/` on the
same TheLake host.

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
