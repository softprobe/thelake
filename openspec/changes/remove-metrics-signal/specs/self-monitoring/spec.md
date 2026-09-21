## REMOVED Requirements

### Requirement: DuckLake self-export of process metrics
~~thelake SHALL export self-monitoring metrics into an ops DuckLake scope via an internal `PushMetricExporter` and `add_metrics`.~~

### Requirement: Ops Prom dashboard over self-metrics
~~thelake SHALL provide a Grafana Prometheus datasource/dashboard querying ops-lake self-monitoring series.~~

### Requirement: Slow-query ops log self-drain
~~thelake SHALL enqueue slow-query events into the ops lake via `add_logs`.~~

## MODIFIED Requirements

### Requirement: OTel Meter collection for self-monitoring
thelake SHALL collect self-monitoring metrics via the OpenTelemetry Meter API with `SdkMeterProvider` + `PeriodicReader` and export them with a **standard OTLP metrics exporter** (not into DuckLake). Destination SHALL use standard `OTEL_EXPORTER_OTLP_*` configuration. Config MAY retain `enabled` and `export_interval_seconds` only.

#### Scenario: Enabled exports via OTLP
- **WHEN** self-monitoring is enabled and an OTLP metrics endpoint is configured
- **THEN** process instruments are periodically exported over OTLP
- **AND** no series are written into customer or ops DuckLake via `add_metrics`

#### Scenario: Disabled stays quiet
- **WHEN** self-monitoring is disabled
- **THEN** no OTLP metrics export loop is started

### Requirement: Labeled cardinality-safe instruments
Self-monitoring metric attributes SHALL remain limited to the existing low-cardinality set (`tenant`, `signal`, `op`, `status`, `sql_kind`, `app`, `table`, `day_kind`, `size_bucket`). Latency series SHALL keep `*_duration_milliseconds_{sum,count}` naming.

#### Scenario: Ingest series still labeled
- **WHEN** customer OTLP **logs** or **traces** ingest succeeds for tenant `T`
- **THEN** process instruments record with `tenant="T"` and the corresponding `signal` label
