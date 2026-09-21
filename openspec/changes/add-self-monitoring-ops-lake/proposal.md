# Change: add-self-monitoring-ops-lake

> **Superseded (DuckLake export):** Product metrics and ops DuckLake metric
> self-export are superseded by
> [`remove-metrics-signal`](../remove-metrics-signal/proposal.md). Process Meter
> instruments and `record_*` call sites remain; export MUST use the standard
> OTLP metrics exporter (`OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*`)
> when `self_monitoring.enabled` is true. Do not write process metrics into
> DuckLake.

## Why
thelake has no first-party self-monitoring: process logs and `/health` SelfHeal only. Operators need OTel-collected runtime metrics exported via standard OTLP, without polluting customer tenant data.

## What Changes
- OTel Meter API + SDK metrics collection inside softprobe-runtime (metrics-first).
- ~~Internal DuckLake exporter into reserved ops scope~~ — **superseded**; use standard OTLP metrics export instead.
- Reserved tenant id `thelake-ops` remains rejected by `POST /v1/tenants` (including exists path) so it cannot collide with customer scopes.
- Auth stub multi-key→tenant map; export-drop informational field on `/health` (no status flip) where still applicable.
- ~~Grafana ops Prom datasource + dashboard~~ — **superseded** with product Prometheus removal.

## Non-goals
- Design 1 localhost OTLP self-loop; Design 3 sidecar / second instance; second Postgres `dbname`.
- DuckLake self-export of process metrics (superseded — use OTLP out).
- Softprobe product promotion on an ops lake.

## Impact
- `thelake` runtime, config, docs.
- Verification: thelake `make ci` (+ workspace gates when auth/dev land).
