---
name: verify-thelake
description: "Drive the thelake HTTP API on a local process to prove health, workspace provisioning, and trace search. Use when a change to thelake ingest, query, or workspace routes needs a live check."
---

# Verify thelake

thelake is a Rust HTTP service. The primary surface is `http://127.0.0.1:18090`. The Explorer UI is a second surface and is not driven here. Two verification processes must not share a port or a catalog schema.

## Launch

From the repo root, after `make setup` has Postgres on `localhost:5432` database `ducklake` and MinIO on port 9000. `make ducklake-extension` must have produced `ducklake.duckdb_extension`, and `ducklake.extension_path` in `CONFIG_FILE` must point at that file. `./dist/ducklake.duckdb_extension` exists only after `make build-release`.

```bash
chmod +x .cursor/skills/verify-thelake/scripts/*.sh .cursor/skills/verify-thelake/scripts/mint_assertion.py
.cursor/skills/verify-thelake/scripts/launch.sh
```

`launch.sh` builds with `cargo run --locked --bin thelake` using `~/.cache/thelake` for Cargo and `libduckdb`, listens on `127.0.0.1:18090` (`VERIFY_THELAKE_PORT`), sets `SOFTPROBE_GRPC_DISABLE=1`, and writes the pid to `/tmp/verify-thelake-run/thelake.pid`. Ready means `GET /health` returns JSON `"status": "ok"`. The log is `/tmp/verify-thelake-evidence/thelake.log`.

Override `CONFIG_FILE` when the check needs `workspace_scope_mode: shared`. The committed `config.yaml` defaults to isolated scope.

Teardown is `scripts/cleanup.sh`. It signals only that pid.

## Doctor

```bash
.cursor/skills/verify-thelake/scripts/doctor.sh
```

Doctor fails unless the recorded pid is alive and `/health` returns `"status": "ok"`. Run it before the first drive and again after a failed drive.

## Drive

Mint a workspace assertion. `workspace_id` must be a UUID.

```bash
TOKEN=$(.cursor/skills/verify-thelake/scripts/mint_assertion.py \
  --workspace-id "$WORKSPACE_ID" \
  --secret "${SOFTPROBE_ASSERTION_HMAC_SECRET:-dev-assertion}")
```

Send `X-Softprobe-Assertion: $TOKEN` on data routes. Send `Authorization: Bearer ${SOFTPROBE_ADMIN_API_KEY:-dev-admin}` on `POST /v1/workspaces`. Do not put `workspace_id` in a data-route body.

Feature recipes live in `features/`.

## Evidence

Write proof under `/tmp/verify-thelake-evidence/`. Keep the request body, the response body, and the HTTP status. A health proof is the doctor stdout. A mutation proof is a second read of the stored trace. `cleanup.sh` does not delete this directory.

## Cleanup

```bash
.cursor/skills/verify-thelake/scripts/cleanup.sh
```

Then confirm `/tmp/verify-thelake-evidence/` still holds the files from the drive.

## Helpers

- `scripts/launch.sh` starts the process.
- `scripts/doctor.sh` checks pid and `/health`.
- `scripts/cleanup.sh` stops the recorded pid.
- `scripts/mint_assertion.py` prints one HS256 assertion.
- `scripts/fixtures/trace.json` is one OTLP JSON trace. `traceId` and `spanId` are lowercase hex. The span starts at `2026-05-03T10:03:20Z`.
