# Current Architecture Decisions

This file contains decisions that define the current runtime architecture.
Superseded Iceberg-era decisions are preserved in
[`legacy/decision-log-iceberg-era.md`](legacy/decision-log-iceberg-era.md).

## ADR-014: DuckLake is the only runtime table format

**Date:** 2026-07-16
**Status:** Accepted

### Context

The former Apache Iceberg design required a REST catalog, Lakekeeper,
snapshot/manifest maintenance, and a parallel query path. The application also
carried an in-memory buffer, staged Parquet tier, and application WAL to reduce
small-file pressure. Those layers increased operational and code complexity.

The runtime already uses DuckDB for writes and queries. DuckLake provides the
catalog, snapshots, data-file management, and DuckDB integration directly.

### Decision

Use DuckLake as the sole durable store for spans and logs.

- PostgreSQL is the production/multi-tenant catalog.
- SQLite is the local multi-client catalog.
- Parquet data lives under the configured local or object-store `data_path`
  when rows are not catalog-inlined.
- Apache Iceberg, Lakekeeper, the staged Parquet tier, and application WAL are
  not supported runtime paths. An optional **soft coalesce** buffer
  (`ingest.flush_interval_seconds`) always sits on the OTLP write path: `0`
  drains immediately after enqueue; `N>0` waits N seconds so posts batch into
  fewer DuckLake commits. That buffer is not a
  WAL or staged query tier.
- Customer product metrics are out of scope (see ADR-016).

### Consequences

- `flush_interval_seconds: 0` still acks inside the coalesce buffer, but
  `IngestEngine::add_*` awaits drain before returning (readable on HTTP 200).
- `flush_interval_seconds: N > 0` batches posts for up to N seconds (first-byte
  deadline) before drain. Crash or post-ack write failure can lose data;
  exporters are not told about background write failures.
- DuckLake data inlining is used to avoid tiny object-store files for normal
  collector batches.
- Query workers ATTACH the same tenant DuckLake scope as ingest.
- Maintenance uses DuckLake merge, snapshot expiry, and old-file cleanup
  procedures.
- Public SQL table names are `traces`, `logs`, and `scores` only. Historical
  Iceberg/buffer aliases are not rewritten.

## Current invariant: catalog backend policy

Use PostgreSQL for production and tenant-scoped operation, and SQLite for local
multi-client development. Reject DuckDB as a DuckLake catalog backend because
it is single-client only.

SQLite's `META_JOURNAL_MODE 'WAL'` is a database journal setting and must not
be described as an application ingest WAL.

## Current invariant: soft coalesce ingest

OTLP always enqueues into `CoalesceBuf` (ack on enqueue) via a channel; a
**single background worker** per signal owns the pending buffer and alone
decides when to drain to DuckLake:

- **`flush_interval_seconds: 0`** — worker flushes as soon as pending is non-empty;
  `IngestEngine::add_*` also awaits that drain before returning (HTTP 200 ⇒
  readable). The coalesce buffer itself still acks on enqueue.
- **`flush_interval_seconds: N > 0`** — worker waits up to N seconds (from first
  byte in the window) unless buffered OTLP bytes hit the eager threshold;
  `add_*` returns after enqueue (ack before durable commit).
- Enqueue only waits when the soft in-memory byte budget is full (backpressure);
  budget is released at drain (before write completes). Soft budget is
  `ingest.buffer_size_mb` (default 256), clamped to absolute ceilings
  (`128 MiB` eager / `256 MiB` max wire bytes).
- DuckLake writes are wrapped with `ingest.write_timeout_seconds` (default 60,
  `0` disables, clamped ≤ 3600) so a hung INSERT fails the flush instead of
  stalling that signal forever. Coalesce does not add its own write watchdog.
- `force_flush` (tests) sends `Flush` and waits until the byte budget is empty.
- Dropping the last `CoalesceBuf` closes the channel; the worker discards
  pending (no WAL) and exits.

Post-ack write failures are logged and dropped — not returned to the exporter.
Unflushed rows may be lost on crash. This is not a WAL or staged tier.

**Schema/DDL off the hot path (locked principle):** Schema creation, validation,
timestamp precision migrations, partition/sort layout, and table options
(`set_option`) run strictly during writer-pool startup/bootstrap, explicit
promotion (`POST /v1/promotions/apply`), or maintenance. The warm INSERT path
executes strictly `BEGIN TRANSACTION; INSERT ...; COMMIT;`. Soft coalesce
amortizes commit frequency when `N > 0`, but is **not** a mitigation for
schema-on-write overhead; warm writes perform zero `DESCRIBE`, partition-info,
or DDL probes regardless of flush interval.

The writer may create a temporary local Parquet file to bridge Arrow into
DuckLake. That file is deleted after commit or failure and is not durable,
queryable, or recoverable storage.

## Current invariant: tenant-bound runtime engines

Resolve tenant identity at authentication/instantiation boundaries and create a
tenant-bound runtime engine containing storage, ingest, query, and optional
session/catalog services. Operational APIs must not accept an arbitrary tenant
or DuckLake scope after binding.

For PostgreSQL catalogs, store each tenant's metadata schema and data path in
the durable scope registry.

## Current invariant: explicit business attributes and tenant promotion

Business identifiers use an application-owned `sp.*` attribute convention.
Softprobe does not invent or auto-promote those keys.

Schema promotion is physical-scope-scoped:

- apply manifests with authenticated `POST /v1/promotions/apply`;
- store active specs in the bound physical-scope metadata schema
  (`promotion_specs`); shared scopes therefore share promotion state;
- add only nullable telemetry columns, extracted on **future** ingest;
- do not configure promotion through process-global `config.yaml`.

Canonical contract: [`promotion.md`](promotion.md).

## ADR-015: Temporary MAP bags; prefer promoted columns (VARIANT deferred)

**Date:** 2026-09-10
**Status:** Accepted (temporary)
**Issue:** [#55](https://github.com/softprobe/thelake/issues/55)

### Context

DuckLake `VARIANT` shredding for hot attribute bags required
`data_inlining_row_limit=0` on Postgres catalogs (VARIANT does not inline) and a
warm-path `::JSON::VARIANT` cast. That write path dominated Softprobe CPU under
full OTEL demo ingest with Grafana `refresh=10s`. Column promotion already
covers query-hot keys.

### Decision

1. Store hot telemetry bags as `MAP(VARCHAR, VARCHAR)` temporarily
   (traces/logs bags).
2. Remove the `::JSON::VARIANT` INSERT bridge; fail-fast if leftover VARIANT.
3. Set catalog-global `data_inlining_row_limit=500` (DuckLake default; MAP is
   Postgres-inline-safe). Softprobe previously used `10_000`, which left too many
   live spans catalog-resident and slowed session detail scans — revised 2026-09-20.
   TWCS wait-for-next-run: do not flush inlined rows every maintenance pass; TWCS
   only merges live Parquet.
4. Ship product-hot promotion manifests; demo/bench apply them. Softprobe does
   not auto-bootstrap promotions on cold start.
5. All Softprobe SQL compilers prefer promoted columns when an active promotion
   matches.
6. Restore VARIANT shredding when DuckLake+Postgres VARIANT inlining (#42) or
   per-table inlining is available.
7. `make bench-demo-cpu-full` must keep Softprobe mean process CPU &lt; 85% of one
   core under full OTLP + Grafana 10s refresh (durable headroom vs cliff-edge
   saturation).

### Consequences

- Operator rebuild for catalogs that still have VARIANT hot columns.
- Query-hot filters should use promotion; bag access remains for ad-hoc keys.
- Canonical notes: [`variant_shredding.md`](variant_shredding.md),
  [`promotion.md`](promotion.md).

## ADR-016: Product metrics removed (traces + logs only)

**Date:** 2026-09-21
**Status:** Accepted
**Related:** `openspec/changes/remove-metrics-signal/`

### Context

Product metrics (OTLP ingest → `metric_*` DuckLake layout → Prometheus/PromQL)
were the most complex signal in thelake and lost value quickly compared to
forever-retained traces and logs. Keeping them forced a large compaction
ladder, PromQL surface, and Grafana Prom compat tax that did not match the
product thesis.

Process instrumentation remains useful for operators, but exporting it back
into thelake (ops DuckLake + Prom) reintroduced the deleted product path.

### Decision

- Remove customer OTLP metrics ingest (`POST /v1/metrics`), live `metric_*`
  write/query/compaction, Prometheus HTTP API, and PromQL.
- Product surface is **OTLP traces + logs** with Loki/Tempo query
  compatibility.
- Retain self-monitoring Meter instruments and `record_*` call sites; export
  via standard OTLP (`OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*`)
  when `self_monitoring.enabled` is true.
- No DuckLake self-export of process metrics.
- Existing `metric_*` tables in a catalog are left **orphaned** (no DROP
  migration).

### Consequences

- Breaking for any client that relied on `/v1/metrics` or Prometheus
  `/api/v1/*`.
- Supersedes DuckLake-export requirements from
  `add-self-monitoring-ops-lake`.
- Docs under `docs/metrics-timeseries-layout.md`,
  `docs/compat/phase1-prometheus.md`, and Prom-focused perf notes are removed
  or marked obsolete.

## Proposed: metrics time-series layout on DuckLake (superseded)

**Date:** 2026-08-15 (redesign after GreptimeDB study; original goals 2026-08-14)
**Status:** **Superseded by ADR-016** — not accepted; product metrics removed.

### Context

Historical proposal for day-sharded postings, skinny samples, 5m/1h ladder,
and `job` collapse on DuckLake. See ADR-016.

### Decision (superseded)

Do not implement. Product metrics and Prometheus are gone.
