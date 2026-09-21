# Temporary MAP bags (VARIANT shredding deferred)

**Status:** Current (temporary)  
**Breaking change:** yes (physical column type)  
**Restore when:** DuckLake + Postgres VARIANT **inlining** lands (see
[`design-ducklake-42-43.md`](design-ducklake-42-43.md) / issue #42).

## Why VARIANT was removed (temporary)

Postgres-backed DuckLake cannot inline `VARIANT` today. Keeping hot bags as
VARIANT forced `data_inlining_row_limit=0` and a warm-path
`JSON Utf8 → Parquet → ::JSON::VARIANT` shredding cast on every flush. That
write tax dominated Softprobe CPU under OTEL demo ingest + Grafana
`refresh=10s`.

Tenant-scoped **column promotion** ([`promotion.md`](promotion.md)) is the
governed fast path for query-hot keys. VARIANT shredding remains desirable as
optional acceleration for unpromoted/ad-hoc keys once upstream inlining works —
it is not load-bearing for the product thesis until then.

## Current physical types

| Table | Columns | Type |
|-------|---------|------|
| `traces` | `attributes`, `resource_attributes`, `instrumentation_scope`, `links` | `MAP(VARCHAR, VARCHAR)` |
| `logs` | `attributes`, `resource_attributes` | `MAP(VARCHAR, VARCHAR)` |
| `scores` / `score_configs` | `metadata` | `MAP(VARCHAR, VARCHAR)` (unchanged) |
| nested `traces.events[].attributes` | | `MAP` (unchanged) |

Orphaned `metric_series.labels` / skinny metric tables from older catalogs are
not a product path.

## Write path

1. Arrow stages hot attribute maps as Arrow **Map** (`Utf8` → `Utf8`).
2. DuckLake `CREATE` / `INSERT` uses `SELECT *` from temp Parquet — **no**
   `::JSON::VARIANT` REPLACE bridge.
3. Promotion extract still fills dedicated typed columns on **future** ingest.

## Inlining re-evaluation

MAP bags are Postgres-inline-safe (scores metadata already inlines). Default
catalog-global `data_inlining_row_limit` is **`500`**. TWCS merges live Parquet
and does **not** flush catalog-inlined rows every pass. Batches over the limit
write Parquet and are compacted on a later maintenance run.

Primary wins of this change: remove VARIANT cast/shredding **and** restore small-batch
inlining for MAP bags.

## Query path

Map field access and JSON projection stay the familiar forms:

```sql
WHERE CAST(attributes['sp.user.id'] AS VARCHAR) = 'user-123'
SELECT CAST(attributes AS JSON) AS attributes FROM traces
```

**Hard rule:** if a matching promoted column is active, Softprobe SQL compilers
MUST prefer it (`COALESCE(promoted_col, CAST(bag['key'] AS VARCHAR), …)` with
promoted first). See [`promotion.md`](promotion.md).

## Operator migration

Existing DuckLake tables created with `VARIANT` hot columns are **not**
auto-migrated. On write, Softprobe fails fast requiring a table/catalog rebuild
when a hot column is still `VARIANT`.

Rebuild options (operator-owned; Softprobe does **not** auto-drop tables):

1. **Dev / local:** recreate DuckLake metadata/data paths, or
   `SPLAKE_RESET_DUCKLAKE=1` if you accept wiping that catalog.
2. **Production:** new metadata schema / data path, re-ingest (or offline copy
   with explicit MAP casts), then cut readers over. Do not mix MAP and VARIANT
   physical types for the same logical table name.

## Related code

- [`src/storage/schema/variant.rs`](../src/storage/schema/variant.rs) — bag SQL helpers + prefer-promoted
- [`src/storage/schema/tables.rs`](../src/storage/schema/tables.rs)
- [`src/storage/ducklake/`](../src/storage/ducklake/) (`writer.rs`, `otlp.rs`, `util.rs`)
- [`docs/promotion/`](promotion/) — product-hot manifests (traces/logs)
- Full-demo CPU gate: `make bench-demo-cpu-full`
