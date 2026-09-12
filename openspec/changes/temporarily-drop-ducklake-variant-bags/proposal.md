# Change: temporarily-drop-ducklake-variant-bags

## Why

Postgres-backed DuckLake cannot inline `VARIANT` today (#42). Softprobe forced
`data_inlining_row_limit=0` so VARIANT bags land in Parquet, and the warm write
path still does JSON Utf8 staging → temp Parquet → `::JSON::VARIANT` shredding.
That dominates ingest cost under OTEL demo + Grafana refresh=10s.

Tenant-scoped column promotion already covers query-hot keys. VARIANT shredding
is optional acceleration for unpromoted keys, not load-bearing for the product
thesis — and it currently owns too much of the CPU budget.

## What Changes

- Temporarily store hot telemetry bags as `MAP(VARCHAR, VARCHAR)` again
  (traces/logs attribute bags + `metric_series.labels`).
- Remove the `::JSON::VARIANT` INSERT bridge and VARIANT type gates.
- Re-enable catalog-global `data_inlining_row_limit=10_000` (MAP is inline-safe).
  Rewrite metrics AC-F7 to wait-for-next-run TWCS (no flush-before-merge).
- Ship product-hot `telemetry_columns` promotion manifests; apply via demo/bench
  hooks (no cold-start auto-bootstrap).
- All Softprobe SQL compilers prefer promoted columns when an active promotion
  matches (`COALESCE(promoted, bag path)` — promoted first).
- Loki/Tempo lean toward promoted / per-key access where matchers allow.
- Add `make bench-demo-cpu-full`: Softprobe process mean CPU &lt; 85% under full
  OTLP ingest + Grafana refresh=10s on one core.

## Non-goals

- Forking DuckLake for Postgres VARIANT inlining (#42).
- Auto-promoting every `sp.*` key.
- Historical backfill of newly promoted columns.
- SoftProbe external `ThelakeSql` adapters (document the contract only).

## Impact

- Breaking physical type change (VARIANT → MAP); operators rebuild catalogs.
- Runtime write/query path, promotion docs, Grafana demo overlays, perf harness.
- Restore when DuckLake+Postgres VARIANT inlining lands (#42) or per-table
  inlining exists.
