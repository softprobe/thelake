# Workspace identity

**Status:** Current contract

## Canonical concepts

| Concept | Meaning |
|---------|---------|
| `workspace_id` | UUID. The only logical tenancy key on the caller token, the engine cache, and fact-table rows. |
| `physical_scope` | Catalog DSN + `metadata_schema` + `catalog_alias` + `data_path` |
| `shared` | Many workspaces use one physical scope. Row filters and temp views use `workspace_id = <uuid>`. |
| `isolated` | One workspace uses its own physical scope. |

## Shared mode

Process config owns the single physical scope. `workspace_for(workspace_id)` requires a `workspace_scope_binding` row and then uses that default warehouse. An unknown UUID is `unknown scope`. Ingest rejects it before writing traces or `session_summary_dirty`.

Admin `POST /v1/workspaces` records workspace UUID → default physical scope. The same lookup serves the provision exists-check, ingest, and the session reducer. The reducer lists workspace ids from `workspace_scope_binding` only. Isolated mode uses the same binding row to select that workspace's own physical scope. Startup records the configured lake as `_default` so readiness can open it. An empty caller id uses that row. It is not a customer workspace, and data routes still require authentication.

## Caller identity

Data routes take `workspace_id` from the authenticated caller. TheLake checks that UUID against `workspace_scope_binding`. It does not read membership or display names. See [authentication](../compat/auth.md).

`POST /v1/workspaces` rejects a non-UUID `workspaceId` (same `parse_workspace_id` contract as auth). Dedicated Grafana and seed harnesses must provision the fixture UUIDs returned by auth mocks (`tests/util/workspace_ids.rs`).

## Provision

`POST /v1/workspaces` checks `Authorization: Bearer <SOFTPROBE_ADMIN_API_KEY>`.

```json
{
  "workspaceId": "11111111-1111-4111-8111-111111111111",
  "storageHints": {
    "ducklakeMetadataSchema": "thelake",
    "ducklakeDataPath": "gs://bucket/prefix/"
  }
}
```

`workspaceId` must be a UUID. Both storage hints are required. In shared mode the stored row points at the configured physical scope. `"status": "created"` is the first binding. `"status": "exists"` means that UUID is already bound to the same warehouse. Hints that name a different warehouse return `409` `workspace_scope_conflict`. The reserved id `thelake-ops` cannot be provisioned.

## Row and API rules

- Fact tables use a `workspace_id` column (see `src/sql/schema/`).
- Operational APIs do not accept a workspace selector after auth binding. Identity comes from the authenticated caller.
- `session_summary` and `session_summary_dirty` are keyed by `(workspace_id, session_id)`. List reads `session_summary`. The reducer loads workspace ids from `workspace_scope_binding` only.

See also [DuckLake access inventory](ducklake-access.md) and [async jobs](../how-to/async-jobs.md).
