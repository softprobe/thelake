---
name: manage-thelake-workspaces
description: Provision and troubleshoot TheLake workspaces and their PostgreSQL-catalog DuckLake scopes.
---

# Manage TheLake workspaces

Read `docs/architecture/workspace-identity.md`,
`docs/architecture/ducklake-access.md`, and the workspace API in
`docs/reference/openapi.yaml` before changing workspace behavior.

## Model

- `workspace_id` is the UUID identity bound from the authenticated assertion.
- Production DuckLake catalogs use PostgreSQL. A physical scope is the catalog
  connection plus metadata schema, catalog alias, and data path.
- In `shared` mode, workspaces share one physical scope and rows are isolated
  by `workspace_id`. In `isolated` mode, each workspace maps to its own
  physical scope.
- Workspace-scoped engines are resolved through `WorkspaceManager` and
  `AppState`; application handlers must not attach DuckLake themselves.

## Provision a workspace

Use the admin-only `POST /v1/workspaces` endpoint with the UUID and required
`storageHints.ducklakeMetadataSchema` and `storageHints.ducklakeDataPath`.
Authenticate with `SOFTPROBE_ADMIN_API_KEY` as described in the OpenAPI and
configuration docs. Choose a unique schema/path pair for an isolated scope.
An identical repeated request is idempotent; conflicting hints return a
conflict. There is no public workspace deletion endpoint—do not delete catalog
metadata or warehouse data manually to simulate one.

For local Explorer runs, a fresh **shared** schema can avoid provisioning; see
`docs/how-to/explorer.md`. Local anonymous mode only binds traffic to its
configured default workspace and is unsafe on a listener reachable by
untrusted users.

## Diagnose scope problems

1. Verify the authenticated `workspace_id` and configured scope mode.
2. Check `/ready` and the runtime logs for catalog attach or scope resolution
   errors.
3. Compare the registered schema and data path with the runtime configuration.
4. Use the supported workspace API and runtime query path. Do not bypass
   `WorkspaceManager`, edit catalog registry tables directly, or issue raw
   DuckDB connections from API handlers.

Never expose Postgres passwords, object-store keys, or workspace/customer data
in logs or examples.
