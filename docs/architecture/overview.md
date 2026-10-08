# Softprobe Runtime Architecture

**Status:** Current
**Storage backend:** DuckLake

## Overview

`thelake` is a Rust service that combines:

- OTLP trace and log ingestion over HTTP
- OTLP trace ingestion over gRPC
- workspace-scoped DuckLake storage and DuckDB queries
- telemetry search and detail APIs
- schema promotion
- optional process self-monitoring (Meter instruments → standard OTLP metrics export)

DuckLake is the only durable telemetry backend. Optional soft coalesce
(`ingest.flush_interval_seconds` > 0) holds rows in memory briefly before a
DuckLake write; default `0` is flush-through.

Customer data signals are traces and logs. Process self-monitoring can export
OTLP metrics to an external collector when enabled; it does not add customer
metric tables or a Prometheus query API.

## Runtime data flow

```text
OTLP HTTP/gRPC request
        |
        v
authenticate and bind workspace
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
  metadata: PostgreSQL catalog
  rows: catalog-inlined or Parquet under data_path
        |
        v
DuckDB query workers ATTACH the same workspace scope
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

1. resolves the workspace's DuckLake scope;
2. applies active telemetry-column promotions;
3. converts records to Arrow using the canonical schemas in
   `src/storage/schema/`;
4. writes a temporary Parquet file;
5. checks out an already-attached DuckDB connection from the scope's writer
   pool;
6. creates the target table if necessary and inserts the rows in one
   transaction;
7. removes the temporary file.

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

## Workspace isolation

Authentication resolves a workspace before operational work begins. A
`WorkspaceContext` is then built and cached for that workspace with:

- a workspace-bound DuckLake metadata schema and data path;
- a workspace-bound writer and query engine.

With a PostgreSQL catalog, `WorkspaceManager` stores scope mappings in the
configured registry schema. Operational APIs do not accept arbitrary workspace or
scope parameters after binding.

### Self-monitoring (process instruments)

When `self_monitoring.enabled` is true, thelake installs OpenTelemetry Meter
instruments (`SdkMeterProvider` + `PeriodicReader`) and exports process metrics
through the **standard OTLP metrics exporter**. Destination is controlled by
`OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*` (HTTP builder). Export
fails soft if the exporter cannot be built; customer HTTP bind is never blocked.

Process metrics are **not** written into DuckLake (no ops-lake self-export, no
customer `metric_*` path). Reserved workspace id `thelake-ops` remains rejected by
`POST /v1/workspaces` and default-lake binding so it cannot collide with customer
scopes.

#### Cardinality rules

Metric attributes only: `tenant`, `signal` (`logs|traces|none`),
`op` (`ingest|write|query|maintenance|job|session_summary`),
`status` (`ok|error|panic`), `sql_kind` (fixed enum),
`app` (OTLP `service.name`, max 64 → `_other`), `table` (maintenance allowlist
or `_` when N/A),
`size_bucket` (`lt_1mb|1_8mb|8_64mb|gte_64mb`), `job_name` / `scope` /
`outcome` / `reason` (async job leases and skips; `job_name` not `job` so
Prometheus does not collide with resource `job` from `service.name`), `step`
(maintenance: `open_attach`, `open_attach_warm`, `expire_snapshots`,
`cleanup_scheduled_files`, `pass_total`; session_summary.reduce: `claim`,
`aggregate`, `upsert`, `ack`, `total`),
`path` (`coalesce|flush_through` on ingest commit).
Resource: `service.name=thelake`.

Instrument **names** are OpenTelemetry dotted identifiers registered in
`src/self_monitoring/instruments.rs` (for example `thelake.ingest.requests`,
`thelake.ingest.duration`, `thelake.write.duration`,
`thelake.ingest.commit.duration`, `thelake.query.duration`,
`thelake.job.duration`, `thelake.maintenance.step.duration`,
`thelake.session_summary.reduce.duration`,
`thelake.self_monitoring.export_drops`). Downstream Prometheus/OTLP converters
may rename/suffix series; treat that file as the catalog, not this page.

Observable / gauge inventory (`register_observables` and related helpers in the
same module) covers process RSS/CPU, query-worker busy counts, pending ingest
batches, writer pool size, async-job wake ms, and self-heal counters.

Snapshot expiration and scheduled-file cleanup outcomes are returned by the SQL
maintenance script and persisted per physical scope. Cleanup runs when
`maintenance.metadata_enabled` is enabled; disabling compaction does not skip
cleanup. Routine maintenance does not delete unscheduled orphan files.

Anti-recursion: never instrument reserved-workspace ingest; inventory uses
uninstrumented one-shot SQL where applicable. OTLP decode failures on
logs/traces increment `thelake.ingest.errors` for customer workspaces.
Slow DuckDB queries (≥200ms) may emit ops log events for operator drill-down
(standard logging / OTLP logs path — not product metrics). Bootstrap is
best-effort and never blocks customer HTTP bind.

## Telemetry tables

DuckLake creates tables lazily from Arrow schemas.

### `traces`

Core columns include:

- correlation: `session_id`, `trace_id`, `span_id`, `parent_span_id`
- tenancy/application: `app_id`, `organization_id`, `workspace_id`
- timing/status: `timestamp`, `end_timestamp`, `status_code`,
  `status_message`
- OTLP data: `attributes`, `events`, `span_kind`, `message_type`
- HTTP data: request method/path/headers/body and response
  status/headers/body

Rows are inserted ordered by `session_id`, `trace_id`, and `timestamp`
(`thelake_otlp_sorted_by` in `src/sql/schema/otlp_layout.sql`; one-clock
partition on calendar day of `timestamp`).

### `logs`

Core columns include `session_id`, timestamps, severity, body, attributes,
resource attributes, trace/span correlation, and event-time `timestamp`
(one-clock; no `record_date`).

### `scores`

Immutable LLM evaluation records are stored separately from spans because an
evaluation commonly arrives after the observed work. A score targets at least
one trace, span, or session and contains one typed numeric, categorical,
boolean, or text value. `score_id` is the workspace-local idempotency key
(with `timestamp` for day prune on lookup).

### `score_configs`

Append-only score schemas (name + data type + optional numeric bounds /
categorical values). `config_id` is the workspace-local idempotency key. There
is no PATCH; replace a config by inserting a new `config_id`.

## Schema promotion

Promotion is applied through authenticated `POST /v1/promotions/apply`, not
process-global YAML. In isolated scope, active manifests live in the workspace
PostgreSQL metadata schema (`promotion_specs`). In shared scope, they live in
the one physical-scope schema and the resulting DDL and ingest extraction are
global to every workspace using that scope.

- **Telemetry columns:** additive nullable columns on `traces` / `logs`.
  Future ingest extracts declared sources into those columns; historical rows
  stay `NULL`.
- **Business tables:** versioned `<table>_vN` tables plus `<table>_current`
  views with evidence anchors. Apply provisions schema today; automatic OTLP
  row materialization is not wired yet.

`sp.*` attributes are an instrumentation convention only. Softprobe does not
auto-promote them. Canonical contract:
[`promotion.md`](../how-to/promotion.md).

## Query path

`src/query/engine.rs` owns a pool of independent DuckDB worker connections.
Every worker loads `httpfs` and DuckLake, configures object-store access, and
ATTACHes the same DuckLake scope used by its workspace-bound writer.

Public query names are `traces`, `logs`, and `scores`. Bare names are expanded
to the workspace's qualified DuckLake catalog table before execution. Internal
catalog and storage names are not part of the query interface.

First-party compilers emit preferred names only. Ingest defaults to
flush-through (optional soft coalesce does not add a queryable buffer tier).

Query surfaces include:

- workspace-bound `POST /v1/query/sql` for internal/debug use;
- telemetry search, details, fields, sessions, and traces endpoints;
- Loki- and Tempo-compatible query APIs (see [`compat/matrix.md`](../compat/matrix.md));
- `GET /v1/data/ducklake-connection` for clients that query DuckLake locally;
- `make duckdb-shell` for local ad hoc access.

See [adhoc DuckDB](../how-to/adhoc-duckdb.md) for the supported interactive
workflow.

## Maintenance

The leased async scheduler invokes one SQL maintenance script per physical
DuckLake scope when compaction or metadata maintenance is enabled (default
interval **60s**). The script reads its configuration and per-table watermarks
from the PostgreSQL registry. It advances a watermark only after its merge
succeeds, then expires snapshots and cleans files scheduled for deletion after
all eligible table merges succeed. `reader_safety_grace_seconds` protects
queries that still read expired snapshots.

For existing `traces`, `logs`, and `scores` tables, it can:

- call `ducklake_merge_adjacent_files` for files created since that table's
  last successful watermark (or all existing files during its bootstrap pass);
- expire old DuckLake snapshots;
- clean files already scheduled for deletion.

DuckLake applies each table's persisted day partition, sort order, output size,
compression, and row-group layout while merging. Routine maintenance does not
delete orphan files.

Operators should still batch OTLP upstream (collector `batch` processor) so
flush-through ingest does not create one tiny file per export.

## Configuration

See the [configuration reference](../reference/config.md). Canonical example:
`config.yaml`. Defaults and validation live in `src/config.rs`.

## Network surfaces

- HTTP listens on `SOFTPROBE_LISTEN_ADDR` when set; otherwise it binds
  `0.0.0.0` with `server.port` (default `8090`). The binary does not use
  `server.host` for its listen address.
- OTLP/gRPC traces listen on `OTEL_GRPC_PORT` (default `4317`), unless
  `SOFTPROBE_GRPC_DISABLE=1`.
- `/v1/*` operational routes require authentication (assertion header or
  Bearer), except `OPTIONS` CORS preflight and workspace provisioning
  (`POST /v1/workspaces`), which validates an admin bearer in the handler.
  Local anonymous mode uses a fixed data-plane allowlist (see root README).
- Auth wiring uses `SOFTPROBE_AUTH_URL` (defaults to a local auth stub URL).

The HTTP product contract is
[`docs/reference/openapi.yaml`](../reference/openapi.yaml), served as
`/openapi.json` (UI at `/swagger`). Loki/Tempo Grafana-compat routes are
documented under [`compat/`](../compat/README.md). Promotion semantics are in
[schema promotion](../how-to/promotion.md).

## Validation

Local pre-merge gate (from the repository root):

```bash
make setup
make ci
```

`make ci` runs `check-fmt`, `lint`, `test`, and (when MinIO/Postgres are up)
`make test-e2e`. `make test-e2e` runs the DuckLake mode matrix (**isolated and
shared**) via `scripts/run-e2e-matrix.sh`.

GitHub Actions (`.github/workflows/ci.yml`) does **not** invoke `make ci`. It
runs `make doctor` → `setup` → `check-fmt` / `lint` / `test`, plus separate
jobs for DuckLake E2E (isolated and shared) and Explorer UI. Release packaging
is `make release` / `release.yml`. Performance suites are manual
(`make test-perf` / `.github/workflows/performance.yml`).

`make test` is unit/lightweight. `make duckdb-shell` is the supported manual
ATTACH smoke.
