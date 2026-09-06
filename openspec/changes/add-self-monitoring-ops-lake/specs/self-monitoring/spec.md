## ADDED Requirements

### Requirement: OTel Meter collection for self-monitoring
thelake SHALL collect self-monitoring metrics via the OpenTelemetry Meter API with `SdkMeterProvider` + `PeriodicReader` + internal `PushMetricExporter` (not Prometheus text scrape as the collection API) and export them on a configurable interval into the ops DuckLake scope.

#### Scenario: Metrics land only in ops scope
- **WHEN** self-monitoring is enabled and export succeeds
- **THEN** series appear under the configured ops `metadata_schema` / `data_path`
- **AND** customer tenant SQL/Prom queries return zero of those ops series

### Requirement: Labeled cardinality-safe instruments
Self-monitoring metric attributes SHALL be limited to `tenant`, `signal`, `op`, `status`, `sql_kind`, `app` (max 64 distinct, overflow `_other`), `table` (maintenance allowlist), `day_kind`, and `size_bucket`. Latency series SHALL use `*_duration_milliseconds_{sum,count}` names (not `*_latency_ms_*`).

#### Scenario: Ingest series carry tenant and signal
- **WHEN** customer OTLP metrics ingest succeeds for tenant `T`
- **THEN** ops Prom shows `thelake_ingest_requests_total{tenant="T",signal="metrics",...}`

### Requirement: Compaction tenant labels match inventory
Compaction pass/wave/orphan/snapshot instruments SHALL label `tenant` with the
registry `scope_id` (same id as `RuntimeEngine.tenant_id`), not
`ducklake.metadata_schema`. Closed-day and open-day waves SHALL both record
`day_kind`.

#### Scenario: Closed-day wave labeled by tenant id
- **WHEN** TWCS runs a closed-day wave for scope `local-dev-tenant`
- **THEN** ops Prom shows `thelake_compaction_waves_total{tenant="local-dev-tenant",day_kind="closed",...}`

### Requirement: Inventory does not pollute query metrics
Table inventory scrapes SHALL execute metadata SQL on a dedicated uninstrumented
connection path (not the customer worker pool / `record_query` path).

#### Scenario: Inventory SQL absent from query duration
- **WHEN** inventory scrape runs for allowlisted tables
- **THEN** those SQL statements do not increment `thelake_query_duration_*` or
  `thelake_slow_queries_total` for the customer tenant

### Requirement: Process and runtime saturation
thelake SHALL export process RSS/VSZ/CPU/thread/disk IO gauges (best-effort via sysinfo) and query queue-wait / workers-busy / ingest-pending / writer-pool gauges.

### Requirement: Slow-query ops events
DuckDB queries whose queue+execute time is ≥ 200ms on customer engines (`counts_toward_liveness`) SHALL increment `thelake_slow_queries_total` and enqueue an ops log event (`event=thelake.slow_query`, body prefixed `thelake.slow_query `). Ops engines do not count toward liveness and MUST NOT emit these events (anti-recursion).

### Requirement: Reserved ops tenant binding
The runtime SHALL bind authenticated tenant id `thelake-ops` to the ops `RuntimeEngine` built from `self_monitoring` config, and SHALL reject `POST /v1/tenants` for `thelake-ops` on every path including idempotent exists.

#### Scenario: Provision reserved id rejected
- **WHEN** an admin calls `POST /v1/tenants` with `tenantId=thelake-ops`
- **THEN** the response is an error with code `reserved_tenant_id`

### Requirement: Ops failures do not trip liveness
Ops query-worker rebuild failures SHALL NOT increment process-global SelfHeal counters that gate `/health` 503. Export drops SHALL be visible on `/health` JSON without changing HTTP status solely due to drops or ops SelfHeal.

#### Scenario: Ops poison leaves health OK
- **WHEN** only ops workers fail rebuild
- **THEN** `/health` remains HTTP 200 (absent customer SelfHeal collapse)

### Requirement: Customer plane starts if ops attach fails
When self-monitoring is enabled and ops ensure/ATTACH fails, the process SHALL still bind and serve customer traffic; self-monitoring degrades (export drops).

#### Scenario: Broken ops path
- **WHEN** ops data path or schema cannot attach at bootstrap
- **THEN** customer ingest and query continue to work
