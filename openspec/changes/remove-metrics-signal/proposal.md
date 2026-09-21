# Change: remove-metrics-signal

## Why

Product metrics (OTLP ingest → `metric_*` DuckLake layout → Prometheus/PromQL) are the most complex signal in thelake and lose value quickly compared to forever-retained traces and logs. Keeping them forces a large compaction ladder, PromQL surface, and Grafana Prom compat tax that does not match the product thesis.

Process instrumentation remains useful for operators, but exporting it back into thelake (ops DuckLake + Prom) reintroduces the deleted product path. Export MUST go out via a standard OTLP metrics exporter instead.

## What Changes

- Remove customer OTLP metrics ingest (`POST /v1/metrics`), `metric_*` schema/write/compaction, Prometheus HTTP API, PromQL, Prom compat/CI/docs.
- Product surface becomes **traces + logs** only (OTLP write + Tempo/Loki query).
- Keep self-monitoring Meter instruments and `record_*` call sites; replace `DuckLakePushExporter` with a standard OTLP metrics exporter (`OTEL_EXPORTER_OTLP_*`).
- Remove ops DuckLake metric self-export, ops-tenant metrics bootstrap knobs, ops Prom dashboard, and slow-query `add_logs` self-drain.
- Existing customer `metric_*` tables are left orphaned (no DROP migration).

## Non-goals

- OTLP logs exporter for slow-query events.
- Destructive cleanup of existing lake `metric_*` data.
- Changes outside the `thelake` repo.

## Impact

- Breaking: `/v1/metrics` and Prometheus `/api/v1/*` go away.
- Runtime, schema registry, compaction, compat matrix, Makefile/CI, docs.
- Supersedes DuckLake-export requirements from `add-self-monitoring-ops-lake`.
