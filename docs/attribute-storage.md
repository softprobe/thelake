# Attribute storage

The runtime stores flexible OpenTelemetry attributes in DuckLake `MAP` columns.
Tenant-declared promotion manifests add typed columns for fields that need a
stable, efficient query path. Promotion affects new writes; existing evidence
remains in its original attribute maps.

## Physical types

| Table | Columns | Type |
|---|---|---|
| `traces` | `attributes`, `resource_attributes`, `instrumentation_scope`, `links` | `MAP(VARCHAR, VARCHAR)` |
| `logs` | `attributes`, `resource_attributes` | `MAP(VARCHAR, VARCHAR)` |
| `scores`, `score_configs` | `metadata` | `MAP(VARCHAR, VARCHAR)` |
| nested `traces.events[].attributes` | | `MAP(VARCHAR, VARCHAR)` |

## Ingest and query

Arrow carries attribute maps as key/value maps through the temporary Parquet
input used by the DuckLake writer. The writer does not convert these columns
to `VARIANT`. Promoted columns are extracted for new rows using the active
manifest.

Map values remain queryable through DuckDB:

```sql
WHERE CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-123'
SELECT CAST(attributes AS JSON) AS attributes FROM traces
```

When a matching promoted column is active, SQL compilers prefer that typed
column. See [schema promotion](promotion.md).

## Catalog compatibility

The PostgreSQL DuckLake catalog is configured with
`data_inlining_row_limit: 500`. DuckLake handles inlining and file maintenance.
The runtime expects the MAP schema above; a catalog with `VARIANT` attribute
columns must be migrated by its operator before the runtime can write to it.
The runtime does not drop or rewrite existing evidence automatically.
