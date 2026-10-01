# Workspace identity

**Status:** Accepted (clean break, 2026-09-30)  
**Rule:** No backward compatibility. New deployments only. DDL is `CREATE` only — never `ALTER TABLE` for this cutover.

## Canonical concepts

| Concept | Meaning |
|---------|---------|
| `workspace_id` | UUID; only logical tenancy key (assertion, engine cache, row column) |
| `physical_scope` | catalog DSN + `metadata_schema` + `catalog_alias` + `data_path` |
| `shared` | many workspaces → one physical scope; isolate with `workspace_id` on rows |
| `dedicated` | one workspace → its own physical scope (former `isolated`) |

A user belongs to one or more workspaces. Each workspace binds to exactly one
physical scope (a `RuntimeEngine`). There is no `lake_scope_id`, `tenant_key`,
or product `tenant` identity.

## Shared mode (weak bind)

Process config owns the single physical scope. `engine_for(workspace_id)` uses
that default without requiring `workspace_scope_binding`. Row filters and
temp views use `workspace_id = <uuid>`.

`workspace_scope_binding` is not on the shared request path (`engine_for`).
Admin `provision_scope` still records workspace UUID → default physical so
maintenance can list provisioned workspace keys. Dedicated mode uses the
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

thelake binds from `workspace_id` only. No `tenant_key`.

`POST /v1/workspaces` rejects non-UUID `workspaceId` (same
`parse_workspace_id` contract as auth). Dedicated Grafana / seed harnesses
must provision the fixture UUIDs returned by auth mocks
(`tests/util/workspace_ids.rs`), not legacy slug labels used as API keys.

## Kill list

- `lake_scope_id` / `ws-…` slug generator
- assertion `tenant_key`
- `TenantInfo.tenant_id` → `workspace_id`
- row column `tenant_id` → tables `CREATE`d with `workspace_id`
- hard shared allowlist on `engine_for`
- `workspace_lake` as lake identity SoT

## Cutover

Brand-new env (new Postgres schemas / data prefix). Bootstrap from rewritten
SQL sources. Copy **traces only**, rewriting old `tenant_id` slug → workspace
UUID. Other signals start empty.
