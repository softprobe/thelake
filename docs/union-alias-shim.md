# Legacy `union_*` SQL alias shim

**Status:** Compatibility retained (Stage 0b.2)  
**Preferred names:** `traces`, `logs`, `metrics`  
**Shim:** `rewrite_reserved_telemetry_view_names` in `src/query/duckdb.rs`

## Why it exists

DuckDB + DuckLake treat some historical identifiers specially. Softprobe rewrites
public/legacy names to neutral `tm_*` aliases, then inlines qualified DuckLake
tables (or the metrics layout JOIN).

## Stage 0b outcome

| Surface | Emits |
|---|---|
| LLM / telemetry / Jaeger-compat compilers | `traces` / `logs` / `metrics` |
| External / ad-hoc SQL via runtime | Still accepts `union_spans`, `union_logs`, `union_metrics`, and historical `committed_*` / `buffer_*` / `staged_*` / `iceberg_*` |

## Shim deletion plan (0b.3) — do not block later stages

Delete the `union_*` and tier-alias pairs from `rewrite_reserved_telemetry_view_names` when **all** are true:

1. No first-party compiler or fixture emits those names (done in Stage 0b).
2. Sp-llm / Explorer / notebooks / runbooks documented on preferred names.
3. A release note + one release of deprecation metrics (optional: count rewrite hits per alias).
4. Grep of softprobe orgs for `union_spans` / `union_logs` / `union_metrics` in checked-in SQL is clean (or owners acknowledged).

Until then, keep the shim and unit tests that assert legacy rewrites still work.

Internal Rust helpers named `union_metrics_layout_*` are **not** the public SQL
shim — they build the metrics layout JOIN and may keep their names.
