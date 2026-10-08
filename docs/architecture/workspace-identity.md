# Workspace identity

**Status:** Current contract

## Canonical concepts

| Concept | Meaning |
|---------|---------|
| `workspace_id` | UUID; only logical tenancy key (assertion, engine cache, row column) |
| `physical_scope` | catalog DSN + `metadata_schema` + `catalog_alias` + `data_path` |
| `shared` | many workspaces → one physical scope; isolate with `workspace_id` on rows |
| `isolated` | one workspace → its own physical scope |

A user belongs to one or more workspaces. Each workspace binds to exactly one
physical scope (a `WorkspaceContext`). There is no `lake_scope_id`, product
`tenant` identity, or assertion `tenant_key`.

## Shared mode (weak bind)

Process config owns the single physical scope. `workspace_for(workspace_id)` uses
that default without requiring `workspace_scope_binding`. Row filters and
temp views use `workspace_id = <uuid>`.

`workspace_scope_binding` is not on the shared request path (`engine_for`).
Admin `POST /v1/workspaces` still records workspace UUID → default physical so
maintenance can list provisioned workspace keys. Isolated mode uses the
registry for workspace UUID → physical scope on every resolve.

## Assertion

```json
{
  "iss": "softprobe-edge",
  "aud": "sp-backend",
  "sub": "<user or key subject>",
  "workspace_id": "<workspaces.id uuid>",
  "roles": ["member"]
}
```

thelake binds from `workspace_id` only.

`POST /v1/workspaces` rejects non-UUID `workspaceId` (same
`parse_workspace_id` contract as auth). Dedicated Grafana / seed harnesses
must provision the fixture UUIDs returned by auth mocks
(`tests/util/workspace_ids.rs`).

## Row and API rules

- Fact tables use a `workspace_id` column (see `src/sql/schema/`).
- Operational APIs do not accept arbitrary workspace selectors after auth
  binding; identity comes from the bearer assertion (or local anonymous mode).
- Reserved id `thelake-ops` cannot be provisioned via `POST /v1/workspaces`.

See also [DuckLake access inventory](ducklake-access.md) and
[async jobs](../how-to/async-jobs.md).
