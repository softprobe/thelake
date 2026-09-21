# Design: remove metrics signal

## Product boundary

thelake stores and queries **traces** and **logs** only. Metrics are not a product signal: no ingest endpoint, no `metric_*` tables on new schemas, no Prometheus query API.

## Process telemetry

`self_monitoring` keeps OTel Meter instruments (counters/histograms/gauges), inventory scrapers, and `record_*` call sites. Collection uses `SdkMeterProvider` + `PeriodicReader` with a **standard OTLP metrics exporter**. Destination is configured via `OTEL_EXPORTER_OTLP_*`. Config retains `enabled` and `export_interval_seconds` only.

Deleted from self-monitoring:

- `DuckLakePushExporter` and ResourceMetrics → domain `Metric` conversion
- ops DuckLake attach / `thelake-ops` metric write path / `ops_metadata_schema` / `ops_data_path`
- slow-query drain into ops lake via `add_logs`
- ops Grafana Prom dashboard

## Orphan tables

Do not DROP existing `metric_*` tables in deployed lakes. New `TABLE_SPECS` omit them; writers and maintenance ignore them.

## Shared helpers

Prometheus label sanitization used by Loki moves to a signal-neutral module before Prom projection is deleted.
