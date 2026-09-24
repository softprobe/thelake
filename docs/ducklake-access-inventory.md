# DuckLake access inventory

This is the source-of-truth inventory for production code that opens, owns, or
receives a DuckDB connection, or reaches the DuckLake writer directly. It is
kept with the code so access-boundary changes can update the inventory in the
same review.

## Connection-owning and connection-consuming code

| Area | Production locations | Current responsibility | Target engine boundary |
| --- | --- | --- | --- |
| Session infrastructure | `src/storage/ducklake/attach.rs`, `src/storage/ducklake/object_store.rs` | Open in-memory DuckDB connections, configure extensions/object storage, and attach DuckLake catalogs | `IngestEngine`, `QueryEngine`, and `MaintenanceEngine` connection factories; shared attach policy remains internal |
| Ingest | `src/storage/ducklake/writer.rs` | Writer pool, catalog initialization, schema setup, and durable OTLP writes | `IngestEngine` |
| Ingest schema support | `src/storage/schema/otlp_layout.rs`, `src/storage/schema/ducklake_partition.rs`, `src/storage/ducklake/util.rs` | Partition/sort probes and ingest-time schema compatibility checks on a supplied connection | `IngestEngine` or an internal storage-schema component owned by it |
| Query | `src/storage/duckdb/engine.rs`, `src/storage/duckdb/cache.rs`, `src/query/workspace_views.rs` | Query connection pool, DuckLake attach, cache setup, isolated table qualification, and shared-mode logical view policy | `QueryEngine` |
| Maintenance | `src/compaction/engine.rs`, `src/compaction/merge.rs`, `src/compaction/session_summary_access.rs`, `src/sql/maintenance/mod.rs` | `MaintenanceEngine` owns physical-scope DuckDB sessions, compaction, metadata cleanup, and the reducer/rebuild facade; SQL recipes in `sql::maintenance` | `MaintenanceEngine` |
| Maintenance implementation | `src/session_summary/reduce.rs` | Claim/ack and summary-domain logic; invokes the internal maintenance access adapter | `MaintenanceEngine` |
| Control plane | `src/storage/ducklake/promotion.rs`, `src/ingest_engine/mod.rs` | Promotion-spec reads and local promotion application using a writer connection | `AdminEngine`; never ordinary workspace query SQL |
| Shared SQL utility | `src/sql/bounds/execute_gate.rs` | Checked execution and preparation on a caller-owned connection | Remains a narrow internal primitive; callers must be engine-owned |

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
| `src/ingest_engine/mod.rs` | `IngestEngine` privately owns the writer and exposes one domain write path per signal (`add_spans`, `add_logs`, `add_scores`, and `add_score_configs`); `AdminEngine` owns promotion operations | Keep domain methods; no direct/batched writer or schema-DDL escape hatch |
| `src/runtime_api.rs` | Runtime API routes promotion operations through the tenant-bound admin facade | Keep promotion access behind the engine/admin capability |
| `src/api/llm/mod.rs` | LLM API reads/writes score and score-config data through `IngestEngine` | Keep workspace binding at the runtime engine boundary |
| `src/main.rs` | Startup starts maintenance via `RuntimeEngineManager` | Keep registry access inside the manager / composition boundary |

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

## Shared-mode startup gates

The first shared-mode release is PostgreSQL-only. Configuration validation
returns `shared_scope_unsupported_backend` for SQLite or any other backend
when `workspace_scope_mode` is `shared`.

Runtime startup must additionally prove that the selected physical scope is
fresh or has completed the explicit scope migration and passes schema
compatibility checks. A deployment must refuse shared-mode startup when that
proof is absent; it must not silently create a second physical scope or infer a
partial migration.

Until the engine migration and those startup checks are implemented, the
configuration validator returns `shared_scope_not_enabled` even for a
PostgreSQL shared-mode configuration. This keeps the option fail-closed while
the contract and inventory work lands.
