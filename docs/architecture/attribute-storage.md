# Attribute storage

The runtime stores flexible OpenTelemetry attributes in DuckLake `VARIANT` columns.
Workspace-declared promotion manifests add typed columns for fields that need a
stable, efficient query path. Promotion affects new writes; existing evidence
remains in its original attribute columns.

## Physical types

| Table | Columns | Type |
|---|---|---|
| `traces` | `attributes`, `resource_attributes`, `instrumentation_scope`, `links`, `events` | `VARIANT` |
| `logs` | `attributes`, `resource_attributes` | `VARIANT` |
| `scores`, `score_configs` | `metadata` | `VARIANT` |

## Ingest and query

Arrow carries attribute bags and `traces.events` as UTF-8 JSON strings through the
temporary Parquet input used by the DuckLake writer. The writer casts staged
JSON strings with `TRY_CAST(col AS JSON)::VARIANT` when loading into DuckLake.
Promoted columns are extracted for new rows using the active manifest.

Variant values remain queryable through DuckDB:

```sql
WHERE CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-123'
SELECT CAST(attributes AS JSON) AS attributes FROM traces
```

Because `duckdb-rs` does not decode raw `VARIANT` columns directly, query
projections cast `VARIANT` columns to `JSON` (`CAST(col AS JSON) AS col`) before
reading them into Rust. When a matching promoted column is active, SQL compilers
prefer that typed column. See [schema promotion](../how-to/promotion.md).

## Catalog compatibility

The PostgreSQL DuckLake catalog is configured with
`data_inlining_row_limit: 500`. DuckLake handles inlining and file maintenance.
The runtime expects the `VARIANT` schema above; a legacy catalog with `MAP`
attribute columns requires a clean cutover (re-creating or migrating the catalog
tables) before the runtime can write to it. The runtime does not drop or
rewrite existing evidence automatically.
