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

Use DuckLake as the sole durable store for spans, logs, and metrics.

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

### Consequences

- `flush_interval_seconds: 0` drains before enqueue returns (OTLP ack ⇒ durable).
- `flush_interval_seconds: N > 0` batches posts for up to N seconds before a
  capped drain (ack before write). Crash or post-ack write failure can lose
  data; exporters are not told about background write failures.
- DuckLake data inlining is used to avoid tiny object-store files for normal
  collector batches.
- Query workers ATTACH the same tenant DuckLake scope as ingest.
- Maintenance uses DuckLake merge, snapshot expiry, and old-file cleanup
  procedures.
- Historical `union_*`, `committed_*`, `buffer_*`, `staged_*`, and
  `iceberg_*` query names may resolve to the same committed DuckLake tables for
  compatibility; they do not imply multiple physical tiers.

## Current invariant: catalog backend policy

Use PostgreSQL for production and tenant-scoped operation, and SQLite for local
multi-client development. Reject DuckDB as a DuckLake catalog backend because
it is single-client only.

SQLite's `META_JOURNAL_MODE 'WAL'` is a database journal setting and must not
be described as an application ingest WAL.

## Current invariant: soft coalesce ingest

OTLP always enqueues into `CoalesceBuf`, then drains to DuckLake:

- **`flush_interval_seconds: 0`** — drain before enqueue returns (OTLP ack ⇒
  durable; same capped write path as timer mode).
- **`flush_interval_seconds: N > 0`** — ack on enqueue; arm a timer so posts
  batch into fewer commits. `force_flush` drains in tests.

For `N > 0`, post-ack write failures are logged and dropped — not returned to
the exporter. Unflushed rows may be lost on crash. This is not a WAL or staged
tier.

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

Schema promotion is tenant-scoped:

- apply manifests with authenticated `POST /v1/promotions/apply`;
- store active specs in the tenant metadata schema (`promotion_specs`);
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
   (traces/logs bags + `metric_series.labels`).
2. Remove the `::JSON::VARIANT` INSERT bridge; fail-fast if leftover VARIANT.
3. Set catalog-global `data_inlining_row_limit=10_000` (MAP is Postgres-inline-safe).
   Rewrite metrics **AC-F7** to wait-for-next-run TWCS: do not flush inlined
   rows every maintenance pass; TWCS only merges live Parquet. Downsample reads
   the DuckLake table (inlined ∪ Parquet), so accuracy is unaffected.
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

## Proposed: metrics time-series layout on DuckLake

**Date:** 2026-08-15 (redesign after GreptimeDB study; original goals 2026-08-14)
**Status:** Proposed — not accepted until the verification report maps every
AC-\* id in [`metrics-timeseries-layout.md`](metrics-timeseries-layout.md).

### Context

The original wide event representation cannot serve Grafana under Astronomy Shop ingest:
mixed-name Parquet files, day-floored snapshot expiry (thousands of live
snapshots), and `max_query_range_seconds = 86400`. Product constraints still
forbid a second TSDB, an application WAL, and deleting tenant data to make
queries fast.

### Decision (proposed)

Keep DuckLake as the only store. **Learn from GreptimeDB** (TWCS, inverted-index *ideas*, metric-engine multiplexing, Flow-style rollups) without forking or embedding it. Split metrics into per-day `metric_series` + `metric_postings` + skinny samples/hist, with **TWCS-shaped** maintenance, 5m/1h downsamples, and `metric_collapse_job_1h`. **Remove** Softprobe-imposed Prom `max_query_range` (retention/TTL bounds data, like Greptime). Expire snapshots at **second** granularity.

**Programmable Softprobe∶Greptime gate (G9):** shared OTLP fixtures; Softprobe_p95 ≤ **10 ×** Greptime_p95 on a pinned query set under `make test-perf` (`COMPARE_GREPTIME=1`). Beating Greptime remains a non-goal. Expected healthy gap ~2–10× (§4.4). Matching Greptime p50 requires reopening G1 or flush-through — not a silent sidecar.

**Query range:** no Softprobe-imposed Prom max (not 90d / not 180d). Like Greptime, retention/TTL decides availability; planner uses 1h/collapse for all windows > 48h. Tested SLO windows remain 30d / 90d / 180d.

### Consequences (if accepted)

- Flush-through ingest still commits once per OTLP request; snapshot **count** is `ceil(age / commit_interval) + headroom`.
- Prom resolves series from day postings (not SST row-group prune) and fails loud at `max_series`.
- SQL names `union_metrics` / `committed_metrics` remain as compatibility relations; Prom must not scan that path.
- Layout + G9 tests live under `make test-perf` (no new public Make target).
- Research clone `./greptime` is reference + **external** bench target only.
- Ready gate: 49 AC ids, `release_full` JSON schema, Greptime ratio rows required.
