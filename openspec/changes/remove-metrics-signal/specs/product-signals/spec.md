## REMOVED Requirements

### Requirement: OTLP metrics product ingest
~~thelake SHALL accept customer OTLP metrics via `POST /v1/metrics` and persist them into DuckLake `metric_*` tables.~~

### Requirement: Prometheus query API
~~thelake SHALL expose Prometheus-compatible HTTP query APIs over stored metric samples for Grafana Prom datasources.~~

### Requirement: Metrics compaction ladder
~~thelake SHALL maintain metrics downsample/collapse tables (`metric_samples_5m`, `metric_samples_1h`, hist rollups, collapse jobs).~~

## ADDED Requirements

### Requirement: Product signals are traces and logs only
thelake SHALL accept and store customer OTLP **traces** and **logs** only. Customer metrics ingest and Prometheus query endpoints MUST NOT be offered.

#### Scenario: Metrics ingest rejected
- **WHEN** a client sends `POST /v1/metrics`
- **THEN** the route is absent (HTTP 404) or otherwise not a metrics ingest handler

#### Scenario: Prometheus API absent
- **WHEN** a client calls `/api/v1/query` or `/api/v1/query_range` on thelake
- **THEN** those Prometheus query routes are not served

#### Scenario: New schemas omit metric tables
- **WHEN** a new tenant DuckLake schema is ensured
- **THEN** `metric_*` tables are not created as part of the product table family
