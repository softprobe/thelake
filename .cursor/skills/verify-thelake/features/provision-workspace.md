# Provision a workspace

An operator creates a workspace with the admin key. Later data calls use an assertion for that workspace id.

## Sub-features

- `provision-create` stores a new workspace and returns its schema and data path.
- `provision-reject` refuses a call with no bearer token.

## How to get to it (user POV)

- `POST http://127.0.0.1:18090/v1/workspaces` with `Authorization: Bearer`.

## Driving it with curl

Preconditions:

- Doctor is ok.
- `SOFTPROBE_ADMIN_API_KEY` is `dev-admin` unless the launch environment set another value.
- `WORKSPACE_ID` is a new UUID.
- `SCHEMA` is `verify_` plus that UUID with the hyphens removed.

- **Create.** Run `curl -sS -D /tmp/verify-thelake-evidence/provision.headers -o /tmp/verify-thelake-evidence/provision.json -X POST http://127.0.0.1:18090/v1/workspaces -H "Authorization: Bearer ${SOFTPROBE_ADMIN_API_KEY:-dev-admin}" -H 'content-type: application/json' -d "{\"workspaceId\":\"$WORKSPACE_ID\",\"storageHints\":{\"ducklakeMetadataSchema\":\"$SCHEMA\",\"ducklakeDataPath\":\"/tmp/verify-thelake-run/data/$SCHEMA\"}}"`. Status is `200`. The body `scope` is the stored catalog. In isolated mode that matches `$SCHEMA` and the requested data path. In shared mode both workspaces return the process `metadata_schema` and `data_path`.
- **Missing token.** Repeat the POST with no Authorization header. Status is `401`.

## Gotchas

- `workspaceId` must be a UUID. A tenant key string is rejected.
- Isolated mode gives this workspace its own catalog. Shared mode needs `workspace_scope_mode: shared` in `CONFIG_FILE` before launch. The response then ignores the requested schema and returns the one process catalog for every workspace.
- A schema name with a hyphen is rejected when Postgres creates it. Strip hyphens from the UUID before using it as `SCHEMA`.
- Schema names longer than 63 bytes fail in Postgres.
