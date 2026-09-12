# Spec: attribute bags (temporary MAP)

## ADDED Requirements

### Requirement: Hot telemetry bags are MAP

Hot attribute bags on `traces`, `logs`, and `metric_series.labels` MUST be
DuckLake `MAP(VARCHAR, VARCHAR)`. The warm write path MUST NOT cast through
`::JSON::VARIANT`.

#### Scenario: traces flush without VARIANT cast
- **WHEN** Softprobe inserts a traces batch
- **THEN** the INSERT SELECT uses plain Parquet columns (MAP) with no
  `::JSON::VARIANT` REPLACE bridge

#### Scenario: leftover VARIANT fails fast
- **WHEN** an existing hot column is still typed VARIANT
- **THEN** ensure/type-gate fails with an operator rebuild message

### Requirement: Prefer promoted columns in generated SQL

When an active `telemetry_columns` promotion matches a source key, product and
compat SQL compilers MUST emit the promoted column before any bag path access
for that key.

#### Scenario: LLM observation type with promotion
- **WHEN** `observation_type` is promoted from `sp.observation.type`
- **THEN** generated LLM SQL COALESCE/filter expressions lead with
  `observation_type`, not `attributes['sp.observation.type']`

### Requirement: Full-demo CPU budget

Under full OTLP demo ingest (traces+logs+metrics) with Grafana dashboard
refresh=10s and Softprobe pinned to one core, mean Softprobe process CPU ratio
MUST be strictly less than 0.85 over the benchmark window (durable headroom
vs a cliff-edge 1.0 gate).

#### Scenario: bench-demo-cpu-full passes
- **WHEN** `make bench-demo-cpu-full` runs against a live full-OTLP stack
- **THEN** the harness records mean CPU &lt; 0.85 and exits 0
