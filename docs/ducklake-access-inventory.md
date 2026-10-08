# DuckLake access inventory

This inventory records the production DuckDB and DuckLake access boundaries.
Workspace engines are obtained through `WorkspaceManager::workspace_for` via
`AppState`.

## Connection-owning and connection-consuming code

| Area | Production locations | Responsibility | Engine boundary |
| --- | --- | --- | --- |
| Session infrastructure | `src/storage/ducklake/attach.rs`, `src/storage/ducklake/object_store.rs` | Configure DuckDB extensions, object storage, and PostgreSQL DuckLake catalog attachments | Storage engine setup |
| Ingest | `src/storage/ducklake/writer.rs` | Writer pool, catalog initialization, schema setup, and durable OTLP writes | Owned by `IngestEngine` |
| Ingest schema support | `src/storage/schema/otlp_layout.rs`, `src/storage/schema/ducklake_partition.rs`, `src/storage/ducklake/util.rs` | Partition and sort probes and ingest-time schema checks on an engine-owned connection | Internal ingest support |
| Query | `src/query/engine.rs`, `src/storage/duckdb/cache.rs`, `src/storage/ducklake/workspace_views.rs` | Query pool, DuckLake attachments, cache setup, and workspace table qualification | Owned by `QueryEngine` |
| Maintenance | `src/compaction/engine.rs`, `src/compaction/merge.rs`, `src/compaction/session_summary_access.rs`, `src/sql/maintenance/mod.rs` | Physical-scope maintenance sessions, file merge, snapshot cleanup, and session-summary jobs | Owned by `MaintenanceEngine` |
| Maintenance implementation | `src/session_summary/reduce.rs` | Claim/ack and summary-domain logic; invokes the internal maintenance access adapter | `MaintenanceEngine` |
| Control plane | `src/storage/ducklake/promotion.rs`, `src/control_plane/admin.rs` | Promotion-spec reads and local promotion application using a writer connection | `AdminEngine`; never ordinary workspace query SQL |
| Shared SQL utility | `src/sql/bounds/execute_gate.rs` | Checked execution and preparation on a caller-owned connection | Narrow internal primitive; callers must be engine-owned |
 
Test-only direct connections are intentionally excluded from this production
inventory. They remain valuable regression coverage and are listed by search
patterns in review rather than converted to engine calls in this contract work.
 
## Direct writer exposure
 
`DuckLakeWriter` is crate-private and is only constructible or consumable by
the ingest composition boundary and the internal DuckLake storage modules. It
is not part of the public workspace API:
 
| Location | Current use | Boundary work required |
| --- | --- | --- |
| `src/storage/mod.rs` | Declares internal storage modules; no public `Storage` wrapper or writer re-export | Keep storage primitives behind engine construction |
| `src/ingest/mod.rs` | `IngestEngine` privately owns the writer and exposes one domain write path per signal (`add_spans`, `add_logs`, `add_scores`, and `add_score_configs`); `AdminEngine` in `src/control_plane/admin.rs` owns promotion operations | Keep domain methods; no direct/batched writer or schema-DDL escape hatch |
| `src/api/control.rs` | Control API routes promotion operations through the workspace-bound admin facade | Keep promotion access behind the engine/admin capability |
| `src/api/scores.rs` | Scores API reads score and score-config data through `QueryEngine` and writes through `IngestEngine` | Keep workspace binding at the workspace context boundary |
| `src/main.rs` | Startup starts maintenance via `WorkspaceManager` | Keep registry access inside the manager / composition boundary |

## Access rules for the refactor

1. `IngestEngine` is the only workspace-facing owner of writes.
2. `QueryEngine` is the only workspace-facing owner of reads. Its raw SQL
   compatibility surface is isolated-mode-only and is rejected by the shared
   scope access policy; generated compatibility queries use the internal
   `TrustedSql` path and resolve shared logical views.
3. `MaintenanceEngine` is the owner of compaction, reducers, schema repair,
   and other physical/admin operations.
4. Attach/bootstrap helpers and checked SQL utilities may accept a raw
   connection only inside those engine implementations or explicitly scoped
   infrastructure modules.
5. A workspace query must not receive a raw DuckDB connection, a physical
   catalog alias, a metadata schema, or a physical table name.
6. Promotion and other control-plane operations must use an explicit admin
   capability and must not be reachable through ordinary workspace SQL.

## Search contract

The following search patterns define the inventory review check:

```text
rg -n 'duckdb::Connection|use duckdb::|Connection::open' src --glob '*.rs'
rg -n 'DuckLakeWriter|\.writer\b' src --glob '*.rs'
```

Expected production hits are limited to the internal DuckLake storage modules,
`IngestEngine`, `QueryEngine`, `MaintenanceEngine`, and composition code. When a new production hit is added,
classify it in this document before the change is considered complete. Test
fixtures may remain direct-connection users when that preserves shared
regression coverage.

## Workspace scope

`WorkspaceManager` owns engine creation and caching by workspace scope.
Application handlers receive engines through `AppState`; they do not open
DuckDB connections or construct catalog attachments themselves. Catalog
metadata uses PostgreSQL in production. See [workspace identity](workspace-identity.md)
for isolated and shared physical scopes.
