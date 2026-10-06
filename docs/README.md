# Documentation

## Runtime

- [Architecture](design.md) — ingestion, storage, query, and maintenance.
- [Workspace identity](workspace-identity.md) — tenancy and physical scopes.
- [Async jobs](async-jobs.md) — leases, dirty-row claims, and scheduled work.
- [Session summaries](session-list-summary.md) — list and rebuild behavior.
- [SQL and schema](design-sql-and-schema.md) — canonical tables and query rules.
- [Event-time layout](design-event-time-layout.md) — timestamp partitioning and pruning.
- [Attribute storage](attribute-storage.md) — MAP columns and promoted fields.
- [DuckLake access inventory](ducklake-access-inventory.md) — connection ownership.

## Guides and API contracts

- [Instrumentation](instrumentation_guide.md)
- [Schema promotion](promotion.md)
- [HTTP API](ingestion-openapi.yaml)
- [Local DuckLake queries](adhoc-duckdb-ducklake.md)

## Compatibility and operations

- [Loki and Tempo compatibility](compat/README.md)
- [Performance](perf/README.md)
- [Product positioning](positioning.md)
- [Runtime goals](goals.md)
