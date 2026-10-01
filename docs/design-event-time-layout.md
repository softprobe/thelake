# Event-time layout for OTLP tables (traces / logs / scores)

**Status:** Design — **one-clock clean cutover** (aligned with [`design-sql-and-schema.md`](./design-sql-and-schema.md)). Prior “keep `record_date` + dual predicates” plan **superseded**.  
**Scope:** DuckLake `traces`, `logs`, `scores`  
**Related:** [`design-sql-and-schema.md`](./design-sql-and-schema.md), [#73](https://github.com/softprobe/thelake/issues/73)

**Constraints:** (1) simplicity (2) clean cutover — one-time copy OK, no compat (3) no room for mistake

---

## 0. Non-negotiable rules

1. **One event time.** DuckLake timestamp columns use `TIMESTAMP_NS` and represent UTC instants. APIs accept readable UTC timestamps and convert them before querying.
2. **No `record_date` / `event_date` / `window_ts` column.** Partition = calendar day of `timestamp` (DuckLake expression locked by greenfield EXPLAIN). A bare `timestamp` predicate is required for partition pruning.
3. **Partition and sort; size row groups independently.** Partition all fact tables by calendar day of `timestamp`; never `PARTITIONED BY (session_id)`. Sort traces, logs, and scores by `(session_id, trace_id, timestamp)`. Use the shared 8 MiB row-group byte-size target. Row groups may contain multiple sessions; session boundaries do not control row-group boundaries. Sorting keeps session values ordered and can tighten row-group statistics without creating a one-session-per-row-group invariant.
4. **Every OTLP read requires `QueryWindow { from, to }`.** No `Option` time. Compilers emit bare `timestamp` predicates via `QueryWindow::scan_with_timestamp_filter` in `src/sql/` (`scan_with_day_filter` only narrows a timestamp sub-window for one calendar day — never a DATE/day-column predicate). Every fact table uses `TIMESTAMP_NS` literals.
5. **All OTLP SQL lives in `src/sql/`.** Handlers call `crate::sql::…`.
6. **Every fact scan carries an explicit, bare `timestamp` bound** for partition pruning. A single lower or upper bound can prune partitions on one side; typed query APIs require a `QueryWindow` and have no all-history default. Before execution, DuckDB's JSON physical plan is checked (query worker `query.sql_gate`, **default on**) to ensure each traces/logs/scores scan receives a conjunctive bare-column timestamp filter. Setting `query.sql_gate: false` skips the entire query-worker fact-scan gate (EXPLAIN and source checks); writer/score-lookup paths stay gated. That is an explicit unsafe latency tradeoff — typed `QueryWindow` builders are not an execute-time substitute. If DuckDB proves a bound redundant from file statistics, the gate accepts it only for a direct query with one fact source; unsupported or ambiguous query forms fail closed. Plans with no fact scan because DuckDB proves the whole result empty are safe. Raw SQL cannot scan Parquet files directly, and each execution call accepts one statement. The execute-time gate also rejects forbidden `record_date` / `event_date` / `window_ts` references.
7. **No backwards compatibility.** Stop writers, export traces from every physical scope into the new schema, validate, then cut over once. Existing logs and scores files remain where they are; their schemas and all subsequent writes/compaction use the shared layout.

Violate any rule → reject the change.

---

## 1. Disease

We stored one fact as two columns (`timestamp` + `record_date`) and partitioned on the second. Authors wrote `WHERE timestamp …` and believed they were pruned. Dual predicates papered over the schema mistake.

---

## 2. Decisions

| # | Decision |
|---|----------|
| D1 | **One clock.** No `record_date` column. Partition expression = day of `timestamp`. |
| D2 | **Query shape:** only `timestamp` lower/upper from `QueryWindow`. |
| D3 | **Cutover:** export traces to the clean schema; validate; flip once. No dual-read. |
| D4 | **Delete** `push_optional_time_bounds` and any optional lake windows. |
| D5 | **One logical event time.** All DuckLake timestamp columns use `TIMESTAMP_NS` and receive the same UTC window as bare-column predicates. |
| D6 | **Session detail window** = summary start/end only. Pad = **0**. |
| D7 | Sort traces/logs/scores by `(session_id, trace_id, timestamp)`. This orders rows; it does not define Parquet row-group boundaries. |
| D8 | **No `app_id` sort lead.** Scores in same layout module. |
| D9 | **Execute gate + `src/sql` locality** — see sql/schema design. |
| D10 | **Inline** default `data_inlining_row_limit = 500`. |
| D11 | Session `/sessions/{session_id}` returns all span `attributes`/`events` with session totals in one response. |

**Pre-cutover (once):** greenfield EXPLAIN with **only** bare `timestamp` predicates must not read out-of-window day files. If prune fails, fix DDL/engine — do **not** reintroduce `record_date`.

---

## 3. Target layout

```sql
-- columns: … payload …, timestamp TIMESTAMP_NS NOT NULL
-- no record_date
ALTER TABLE traces SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
ALTER TABLE traces SET SORTED BY (session_id, trace_id, timestamp);
```

The shared physical profile sets an 8 MiB row-group byte-size target and a 128 MiB file-size target. Both are size targets. Row groups can contain multiple session IDs; the sort order does not add session-specific row-group boundaries. A session that crosses a UTC day boundary necessarily spans day partitions.

```sql
WHERE timestamp >= … AND timestamp <= … AND <identity>
```

Locked by `tests/integration/one_clock_prune.rs` / [`fixtures/one-clock-prune-explain.md`](./fixtures/one-clock-prune-explain.md).
---

## 4. API / product

| Endpoint | Window |
|----------|--------|
| Session detail / spans / recording | `session_summary` start/end only |
| Search | Request `from`/`to` required |
| Trace / observation by id | Require window; 400 if missing |

---

## 5. Acceptance

1. No `record_date` in OTLP schemas.  
2. Every OTLP recipe: bare `timestamp` predicates only; no `record_date` / `event_date` / `window_ts` tokens.
3. Greenfield EXPLAIN: out-of-window days not read.  
4. SQL only under `src/sql/`; D12 execute gate green.  
5. Cutover done; old catalog gone.  
6. No dual-read.

---

## 6. Explicitly rejected

- Keeping `record_date` “for prune” while filtering `timestamp`.  
- Dual predicates forever.  
- In-place dual-read / feature flags.  
- `(year, month, day)` **without** a greenfield prune proof — now locked by EXPLAIN fixture.  
- Optional lake time bounds.  
- Relying on review instead of `src/sql` filter builders, recipe tests, and the execute gate.
- Reintroducing `record_date` because triples feel complex.

**This document and [`design-sql-and-schema.md`](./design-sql-and-schema.md) are the law.** Prior DATE-column + dual-predicate revisions are obsolete.

---

## 7. Shared Parquet layout profile

### Required physical contract

Traces, logs, and scores partition by `year(timestamp), month(timestamp), day(timestamp)`, sort by `(session_id, trace_id, timestamp)`, and use an 8 MiB row-group byte target and a 128 MiB file target. The targets govern physical sizes; no session-to-row-group mapping is required. Row groups may contain several adjacent session IDs because data is sorted by session.

### One profile across all writers

`src/sql/schema/otlp_layout.sql` is the only source for OTLP physical settings: day partition expression, sort tuple, 8 MiB row-group byte target, 128 MiB target file size, Zstandard compression at level 3, and the 500-row inline threshold. Rust initialization/ingest, inline-data materialization, compaction, and the one-time exporter must load this profile. DuckLake table-scoped options persist it; compaction reapplies it through the same helper and fails if that fails. No writer path may carry its own defaults.

Keep Parquet statistics enabled and inspect them in the acceptance tests. DuckDB can write Bloom filters for supported primitive/string columns when it selects dictionary encoding; there is no separate DuckLake column allowlist setting to maintain, so do not invent one. Verify Bloom-filter presence and probe `trace_id`/promoted scalar lookups on production-shaped output before claiming a benefit. Bloom filters complement sorted row-group statistics.

Base table schemas are defined in `src/sql/schema/*.sql`. Runtime Arrow schemas are derived from those DDLs; the only runtime columns added after initialization come from active promotion manifests. There is no historical schema-migration or old-schema read path.

Inlining remains at 500 rows to avoid a Parquet file for every small write. DuckLake applies the persisted table profile when inline rows are flushed. Include inline data in query and flush measurements.

### Compaction and export

Compaction applies the same table-scoped profile as ingest and inline flush, then uses DuckLake's sorted merge. The untracked `tmp/export_production_traces.py` exporter reads every physical scope once, sorts by the shared profile, writes day-partitioned Parquet using the same byte/file targets and compression, applies the SQL base schema plus active promotions, validates each output, and registers the files in the new DuckLake tables. The inventory must include the default physical scope and each distinct shared/isolated physical scope exactly once. Keep writers stopped through inventory, export, validation, and the catalog/config cutover.

### Acceptance

Inspect active files after ingest, inline flush, compaction, and export. Assert row-group byte target behavior, compression, timestamp/session statistics, and schema parity across all paths. For production-shaped datasets above 500 MiB, measure session detail latency and bytes read, broad-scan latency, file counts, and inline backlog before and after cutover. A day-partition EXPLAIN test alone does not establish session-query performance.
