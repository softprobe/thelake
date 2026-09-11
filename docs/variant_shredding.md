# Temporary MAP bags (VARIANT shredding deferred)

**Status:** Current (temporary)  
**Breaking change:** yes (physical column type)  
**Restore when:** DuckLake + Postgres VARIANT **inlining** lands (see
[`design-ducklake-42-43.md`](design-ducklake-42-43.md) / issue #42), or DuckLake
gains per-table inlining so skinny metrics can stay on Parquet while bags inline.

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
| `metric_series` | `labels` | `MAP(VARCHAR, VARCHAR)` |
| `scores` / `score_configs` | `metadata` | `MAP(VARCHAR, VARCHAR)` (unchanged) |
| nested `traces.events[].attributes` | | `MAP` (unchanged) |

Skinny `metric_samples` / hist / postings have **no** attribute bags.

## Write path

1. Arrow stages hot attribute maps as Arrow **Map** (`Utf8` → `Utf8`).
2. DuckLake `CREATE` / `INSERT` uses `SELECT *` from temp Parquet — **no**
   `::JSON::VARIANT` REPLACE bridge.
3. Promotion extract still fills dedicated typed columns on **future** ingest.

## Inlining re-evaluation

MAP bags are Postgres-inline-safe (scores metadata already inlines when
enabled). Catalog-global `data_inlining_row_limit` remains **`0`** because
metrics layout **AC-F7** requires zero inlined bytes for skinny
`metric_samples` / `metric_hist_samples` / `metric_postings` so TWCS merge
sees Parquet. Re-enabling `10_000` would inline those skinny tables too.

Primary win of this change is removing VARIANT cast/shredding, not re-enabling
inlining. Revisit when per-table inlining or #42 VARIANT inlining exists.

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
- [`src/storage/ducklake/`](../src/storage/ducklake/) (`writer.rs`, `otlp.rs`, `util.rs`, `metrics_layout_write.rs`)
- [`docs/promotion/`](promotion/) — product-hot manifests
- Full-demo CPU gate: `make bench-demo-cpu-full`
