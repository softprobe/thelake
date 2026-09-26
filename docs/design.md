# Softprobe Runtime Architecture

**Status:** Current
**Storage backend:** DuckLake
**Last verified against:** `src/` on 2026-09-21

## Overview

`thelake` is a Rust service that combines:

- OTLP trace and log ingestion over HTTP
- OTLP trace ingestion over gRPC
- tenant-scoped DuckLake storage and DuckDB queries
- telemetry search and detail APIs
- schema promotion
- optional process self-monitoring (Meter instruments → standard OTLP metrics export)

DuckLake is the only durable telemetry backend. Apache Iceberg, the staged
Parquet tier, and application WAL have been removed. Optional soft coalesce
(`ingest.flush_interval_seconds` > 0) may hold rows in memory briefly before a
DuckLake write; default `0` is flush-through. Historical documents for removed
designs are under [`legacy/`](legacy/README.md).

Product metrics (customer OTLP `/v1/metrics`, `metric_*` live path, Prometheus /
PromQL) have been removed. Existing `metric_*` tables in a catalog, if any, are
orphaned and are not part of the product surface.

## Runtime data flow

```text
OTLP HTTP/gRPC request
        |
        v
authenticate and bind tenant
        |
        v
decode OTLP -> Span / Log
        |
        +---- flush_interval_seconds == 0 (default) ----+
        |                                               |
        |  soft coalesce N>0: enqueue, return 200       |
        |  (timer / force_flush later)                  |
        |                                               |
        v                                               v
Arrow RecordBatch -> temporary local Parquet
        |
        v
DuckDB transaction (warm path):
  BEGIN TRANSACTION
  INSERT ... SELECT read_parquet(...)
  COMMIT
  (table creation, layout & options ensured at startup/bootstrap)
        |
        v
DuckLake
  metadata: PostgreSQL (production) or SQLite (local)
  rows: catalog-inlined or Parquet under data_path
        |
        v
DuckDB query workers ATTACH the same tenant scope
```

**Default** (`ingest.flush_interval_seconds: 0`): each OTLP request is written
through immediately; upstream OpenTelemetry collectors own batching. Exhausted
DuckLake write retries surface as HTTP `503` so exporters can retry.

**Soft coalesce** (`flush_interval_seconds` > 0): OTLP returns after enqueue; a
background timer flushes coalesced batches. Post-ack write failures are logged
and dropped (not returned to the exporter). Unflushed rows may be lost on crash.
Soft coalesce amortizes commit frequency, but is not a mitigation for
schema-on-write; the warm ingest hot path performs zero schema/DDL probes.

The temporary Parquet file is only an input adapter between Arrow and DuckLake
and is deleted after the transaction; it is not a staged durability tier.

DuckLake data inlining decides where committed rows live:

- batches at or below `ducklake.data_inlining_row_limit` may stay in the
  metadata catalog (default **500**; MAP bags are Postgres-inline-safe);
- larger writes become Parquet files under `ducklake.data_path`.

Both forms are committed DuckLake data and are queried through the same
attached catalog.

## Storage and catalog

The writer in `src/storage/ducklake/` (`writer.rs` plus domain modules
`otlp.rs`, `scores.rs`, `promotion.rs`) is the sole durable writer. It:

1. resolves the tenant's DuckLake scope;
2. applies active telemetry-column promotions;
3. converts records to Arrow using the canonical schemas in
   `src/storage/schema/`;
4. writes a temporary Parquet file;
5. checks out an already-attached DuckDB connection from the scope's writer
   pool;
6. creates the target table if necessary and inserts the rows in one
   transaction;
7. removes the temporary file.

Supported catalog backends:

- `postgres`: production and tenant-scoped deployments;
- `sqlite`: local multi-client development;
- `duckdb`: rejected because DuckLake documents it as single-client only.

SQLite uses `META_JOURNAL_MODE 'WAL'` and a busy timeout. This is SQLite's
catalog journal mode, not the removed Softprobe application WAL.

DuckLake's own conflict retry settings are pinned on writer connections.
The runtime sets `ducklake_max_retry_count=10`,
`ducklake_retry_backoff=1.5`, and `ducklake_retry_wait_ms=100`. Under default
flush-through, exhausted ingest writes are surfaced to the HTTP exporter as
`503 Service Unavailable`. Under soft coalesce, the HTTP ack already happened;
background write failures are WARN-logged only. Softprobe does not add another
hidden write retry loop.

Each catalog scope owns a pool of already-attached writer connections.
`ducklake.writer_pool_size` defaults to `4` and is clamped to `1..=16`.
Writes run on Tokio's blocking pool so PostgreSQL and object-store waits do not
pin async workers.

## Tenant isolation

Authentication resolves a tenant before operational work begins. A
`RuntimeEngine` is then built and cached for that tenant with:

- a tenant-bound DuckLake metadata schema and data path;
- a tenant-bound writer and query engine.

With a PostgreSQL catalog, `RuntimeEngineManager` stores scope mappings in the
configured registry schema. Operational APIs do not accept arbitrary tenant or
scope parameters after binding.

### Self-monitoring (process instruments)

When `self_monitoring.enabled` is true, thelake installs OpenTelemetry Meter
instruments (`SdkMeterProvider` + `PeriodicReader`) and exports process metrics
through the **standard OTLP metrics exporter**. Destination is controlled by
`OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*` (HTTP builder). Export
fails soft if the exporter cannot be built; customer HTTP bind is never blocked.

Process metrics are **not** written into DuckLake (no ops-lake self-export, no
customer `metric_*` path). Reserved tenant id `thelake-ops` remains rejected by
`POST /v1/tenants` and default-lake binding so it cannot collide with customer
scopes.

#### Cardinality rules

Metric attributes only: `tenant`, `signal` (`logs|traces|none`),
`op` (`ingest|write|query|maintenance|export|compact|job|session_summary`),
`status` (`ok|error|panic`), `sql_kind` (fixed enum),
`app` (OTLP `service.name`, max 64 → `_other`), `table` (maintenance allowlist
or `_` when N/A), `day_kind` (`open|closed`),
`size_bucket` (`lt_1mb|1_8mb|8_64mb|gte_64mb`), `job` / `scope` / `outcome`
(async job leases), `step` (maintenance: `open_attach`, `backlog_probe`,
`partition_stats`, `twcs_closed`, `twcs_open`, `expire_snapshots`,
`orphan_cleanup`, `pass_total`; session_summary.reduce: `claim`, `aggregate`,
`upsert`, `ack`, `total`), `path` (`coalesce|flush_through` on ingest commit).
Resource: `service.name=thelake`.

Latency instrument names use `*_duration_milliseconds_{sum,count}` style.

Orphan remove and snapshot expire counters are emitted **only when the action is
enabled/attempted** (`maintenance.metadata_enabled` / `remove_orphan_files_enabled`).
Disabled passes mint nothing (never `status=ok`). Attempted success → `ok`;
attempted failure (`ActionStatus::Failed`) → `error`.

#### Instrument catalog (locked)

| Name | Type | Labels |
|------|------|--------|
| `thelake_ingest_requests_total` / `thelake_ingest_errors_total` | counter | tenant, signal, status, app |
| `thelake_ingest_duration_milliseconds_{sum,count}` | hist | tenant, signal, app |
| `thelake_write_duration_milliseconds_{sum,count}` | hist | tenant, signal |
| `thelake_ingest_commit_duration_milliseconds_{sum,count}` | hist | tenant, signal, path |
| `thelake_ingest_commits_total` / `rows_committed` / `coalesce_flushes` | counter | tenant, signal, path |
| `thelake_query_duration_milliseconds_{sum,count}` | hist | tenant, sql_kind |
| `thelake_query_queue_wait_milliseconds_{sum,count}` | hist | tenant, sql_kind |
| `thelake_slow_queries_total` | counter | tenant, sql_kind |
| `thelake_table_live_files` / `live_bytes` / `open_day_live_files` | gauge | tenant, table |
| `thelake_table_files_by_size_bucket` | gauge | tenant, table, size_bucket |
| `thelake_compaction_passes_total` | counter | tenant, status |
| `thelake_compaction_waves_total` | counter | tenant, table, day_kind |
| `thelake_compaction_duration_milliseconds_{sum,count}` | hist | tenant, table, day_kind |
| `thelake_compaction_files_before` / `files_after` | gauge | tenant, table, day_kind |
| `thelake_orphan_remove_total` | counter | tenant, status |
| `thelake_snapshot_expire_total` | counter | tenant, status |
| `thelake_job_duration_milliseconds_{sum,count}` | hist | job, scope, status |
| `thelake_maintenance_step_duration_milliseconds_{sum,count}` | hist | scope, step, table |
| `thelake_session_summary_reduce_duration_milliseconds_{sum,count}` | hist | tenant, step |
| `thelake_session_summary_dirty_upsert_duration_milliseconds_{sum,count}` | hist | tenant |
| `thelake_async_jobs_wake_ms` | gauge | — |
| `thelake_self_heal_rebuilds_total` / `thelake_self_heal_consecutive_failures` | counter/gauge | — |
| `thelake_process_*` (RSS/VSZ/CPU/threads/disk) | gauge/counter | — |
| `thelake_query_workers` / `workers_busy` / `ingest_pending_batches` / `writer_pool_size` | gauge | — |
| `thelake_self_monitoring_export_drops_total` | counter | — |

Anti-recursion: never instrument reserved-tenant ingest; inventory uses
uninstrumented one-shot SQL where applicable. OTLP decode failures on
logs/traces increment `thelake_ingest_errors_total` for customer tenants.
Slow DuckDB queries (≥200ms) may emit ops log events for operator drill-down
(standard logging / OTLP logs path — not product metrics). Bootstrap is
best-effort and never blocks customer HTTP bind.

## Telemetry tables

DuckLake creates tables lazily from Arrow schemas.

### `traces`

Core columns include:

- correlation: `session_id`, `trace_id`, `span_id`, `parent_span_id`
- tenancy/application: `app_id`, `organization_id`, `tenant_id`
- timing/status: `timestamp`, `end_timestamp`, `status_code`,
  `status_message`
- OTLP data: `attributes`, `events`, `span_kind`, `message_type`
- HTTP data: request method/path/headers/body and response
  status/headers/body

Rows are inserted ordered by `app_id`, `session_id`, and `timestamp`
(one-clock partition on calendar day of `timestamp`).

### `logs`

Core columns include `session_id`, timestamps, severity, body, attributes,
resource attributes, trace/span correlation, and event-time `timestamp`
(one-clock; no `record_date`).

### Orphaned `metric_*` tables (not product)

Customer metrics ingest and the Prometheus/PromQL surface are removed. Catalogs
that previously wrote `metric_samples` / related tables may still contain those
objects; Softprobe does not DROP them and does not treat them as a live product
path. New installs should not create them.

### `scores`

Immutable LLM evaluation records are stored separately from spans because an
evaluation commonly arrives after the observed work. A score targets at least
one trace, span, or session and contains one typed numeric, categorical,
boolean, or text value. `score_id` is the tenant-local idempotency key.

### `score_configs`

Append-only score schemas (name + data type + optional numeric bounds /
categorical values). `config_id` is the tenant-local idempotency key. There is
no PATCH; replace a config by inserting a new `config_id`. Human annotation
(Annotate panel → scores) is documented in Softprobe LLM `docs/annotation.md`.

## Schema promotion

Promotion is applied through authenticated `POST /v1/promotions/apply`, not
process-global YAML. In isolated scope, active manifests live in the workspace
PostgreSQL metadata schema (`promotion_specs`). In shared scope, they live in
the one physical-scope schema and the resulting DDL and ingest extraction are
global to every workspace using that scope. SQLite supports promotion in its
configured local single-scope DuckLake catalog.

- **Telemetry columns:** additive nullable columns on `traces` / `logs`.
  Future ingest extracts declared sources into those columns; historical rows
  stay `NULL`.
- **Business tables:** versioned `<table>_vN` tables plus `<table>_current`
  views with evidence anchors. Apply provisions schema today; automatic OTLP
  row materialization is not wired yet.

`sp.*` attributes are an instrumentation convention only. Softprobe does not
auto-promote them. Canonical contract:
[`promotion.md`](promotion.md).

## Query path

`src/query/duckdb.rs` owns a pool of independent DuckDB worker connections.
Every worker loads `httpfs` and DuckLake, configures object-store access, and
ATTACHes the same DuckLake scope used by its tenant-bound writer.

Public query names: `traces`, `logs`, and `scores`. Bare names are expanded to
the tenant's qualified DuckLake catalog table before execution. Historical
Iceberg/buffer aliases (`union_*`, `committed_*`, `buffer_*`, `staged_*`,
`iceberg_*`) are not rewritten.

First-party compilers emit preferred names only. Ingest defaults to
flush-through (optional soft coalesce does not add a queryable buffer tier).

Query surfaces include:

- tenant-scoped `POST /v1/query/sql` for internal/debug use;
- telemetry search, details, fields, sessions, and traces endpoints;
- Loki- and Tempo-compatible query APIs (see [`compat/matrix.md`](compat/matrix.md));
- `GET /v1/data/ducklake-connection` for clients that query DuckLake locally;
- `make duckdb-shell` for local ad hoc access.

See [`adhoc-duckdb-ducklake.md`](adhoc-duckdb-ducklake.md) for the supported
interactive workflow.

## Maintenance

The scheduler runs when compaction or metadata maintenance is enabled
(default interval **300s**). It walks the default DuckLake scope and all
registered tenant scopes. Merge calls retry through serialization conflicts (8
attempts × 2 waves). After each scope pass, Softprobe logs when Parquet
file counts remain high (≥200).

For `traces`, `logs`, and `scores`, it can:

- set the configured target file size;
- call `ducklake_merge_adjacent_files`;
- expire old DuckLake snapshots;
- clean old DuckLake files.

Operators should still batch OTLP upstream (collector `batch` processor) so
flush-through ingest does not create one tiny file per export.

Iceberg manifest rewrite and Iceberg REST catalog maintenance do not
exist in the current path.

## Configuration

The canonical shape is `config.yaml`; defaults and validation live in
`src/config.rs`.

Important DuckLake settings:

- `metadata_path`: PostgreSQL connection string (the DuckLake catalog is always Postgres)
- `data_path`: local, `s3://`, or `gs://` data location
- `catalog_alias`
- `metadata_schema`
- `data_inlining_row_limit` (default `500`; set `0` only when a fixture needs Parquet-per-batch)
- `writer_pool_size` (default `4`, clamped to `1..=16`)

Non-secret object-store settings live in the `object_store` section (`region`
and an optional custom `endpoint` for MinIO/R2). Object-store credentials are
never stored in YAML; they are resolved from the environment: `AWS_ACCESS_KEY_ID`
/ `AWS_SECRET_ACCESS_KEY` (with optional `AWS_SESSION_TOKEN`) for `s3://` paths,
and GCS HMAC interoperability credentials `GCS_HMAC_ACCESS_KEY_ID` /
`GCS_HMAC_SECRET` (with `GCP_HMAC_*` aliases) for `gs://` paths.

Config precedence is:

1. supported environment overrides;
2. `CONFIG_FILE` (default `config.yaml`);
3. built-in defaults when the file does not exist.

Supported direct overrides in `src/config.rs` are `PORT`, `S3_REGION`, and
`SOFTPROBE_MAX_HTTP_BODY_BYTES`.

## Network surfaces

- HTTP listens on `SOFTPROBE_LISTEN_ADDR` when set; otherwise it binds
  `0.0.0.0` with `server.port` (default `8090`). The current binary does not
  use `server.host` for its listen address.
- OTLP/gRPC traces listen on `OTEL_GRPC_PORT` (default `4317`), unless
  `SOFTPROBE_GRPC_DISABLE=1`.
- `/v1/*` operational routes require bearer authentication, except tenant
  provisioning which performs its own admin-token validation.
- Auth wiring uses `SOFTPROBE_AUTH_URL` (defaults to a local auth stub URL).

The implemented HTTP product contract is
[`docs/ingestion-openapi.yaml`](ingestion-openapi.yaml), served live as
`/openapi.json` (UI at `/swagger`). Loki/Tempo Grafana-compat routes are
implemented outside that document. Promotion semantics are in
[`promotion.md`](promotion.md).

## Validation

From the repository root:

```bash
make setup
make ci
```

CI on GitHub runs the same Make entry points (`make ci` after
`make setup`; see `.github/workflows/ci.yml` — fmt, lint, `test`, and `test-e2e`;
release packaging is `make release` / `release.yml`). Performance suites are
manual (`make test-perf` / `.github/workflows/performance.yml`).

`make test` is unit/lightweight; `make test-e2e` is isolated MinIO/PostgreSQL
integration. `make duckdb-shell` is the supported manual ATTACH smoke.
