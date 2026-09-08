# Change: add-self-monitoring-ops-lake

## Why
thelake has no first-class self-monitoring: process logs and `/health` SelfHeal only. Operators need OTel-collected runtime metrics in an isolated DuckLake scope with a Grafana Prom dashboard, without polluting customer tenant data.

## What Changes
- OTel Meter API + SDK metrics collection inside softprobe-runtime (metrics-first).
- Internal DuckLake exporter (bypass public OTLP) into reserved ops scope on the **same** catalog DSN (`ops_metadata_schema` + `ops_data_path`).
- Reserved tenant id `thelake-ops`: `engine_for` bind; `POST /v1/tenants` rejects it (including exists path).
- Ops query workers do not count toward `/health` liveness SelfHeal.
- Auth stub multi-key→tenant map; export-drop informational field on `/health` (no status flip).
- Grafana ops Prom datasource + dashboard (PromQL subset only).

## Non-goals
- Design 1 localhost OTLP self-loop; Design 3 sidecar / second instance; second Postgres `dbname`.
- P0 traces/logs self-export; P0 Loki/Tempo ops dashboards.
- Softprobe product promotion on the ops lake.

## Impact
- `thelake` runtime, config, docs, compat grafana fixtures.
- Workspace `backend` auth stub + `dev` compose env for key→tenant map.
- Verification: thelake `make ci` + workspace `make build && make e2e` when auth/dev land.
