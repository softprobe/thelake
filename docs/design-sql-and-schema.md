# Design: SQL compilation + one-clock schema

**Status:** Canonical SQL DDL and shared OTLP Parquet profile
**Constraints:** (1) simplicity (2) clean cutover — one-time copy OK, no compat (3) no room for mistake
**Related:** [`design-event-time-layout.md`](./design-event-time-layout.md)

Product metrics / Prometheus recipes are **out of scope** (removed). Orphaned
`metric_*` tables, if present in an old catalog, are not a live SQL product path.

---

## 0. Non-negotiable rules

1. **One event-time column named `timestamp` on every DuckLake fact table.**
   A bare predicate on that column enables partition pruning. Physical layout = calendar day of `timestamp`.
   **No `record_date` / `event_date` / `window_ts`.**
   The logical clock is UTC event time. Every DuckLake timestamp column uses
   `TIMESTAMP_NS`. APIs accept readable timestamps and convert them to UTC
   nanosecond literals before querying. Every recipe filters the bare
   `timestamp` column with the same literal type.
2. **One window type: `QueryWindow { from, to }`.** Recipes call a `scan_with_*_filter` method on it.
   Loki/Tempo clocks convert at the protocol edge only — not a second lake
   window type.
3. **All production SQL under `src/sql/` only.** Tests are the only exception. Locality unit test is hard-fail.
4. **Handlers / planners / writers never embed SQL verbs.** They call `crate::sql::…`.
5. **One escaping / quoting API** under `src/sql/`.
6. **Schema registry in `src/sql/schema` (types + DDL).**
7. **Clean cutover.** Stop writers, export traces into the clean schema from every physical scope, validate, then flip once. Existing logs and scores files remain in place. No dual-read / feature flags.
8. **Global scan rule:** every fact scan has an explicit, bare `timestamp` bound for partition pruning. One lower or upper bound can prune partitions on one side. Typed query APIs require a `QueryWindow`; there is no all-history default. A DuckDB JSON physical-plan check runs before query-worker execution when `query.sql_gate` is true (**default**); set `query.sql_gate: false` to skip that EXPLAIN on query workers (writer/score-lookup paths remain gated). The check requires a conjunctive bare-column timestamp filter on every traces/logs/scores scan, including nested queries, DML, CTAS, COPY, and relation commands. When statistics prove the timestamp bound redundant, the gate accepts that only for a direct query with one fact source; when the optimizer proves the entire result empty, there is no fact scan to gate. Raw SQL cannot read Parquet files directly; the writer has a separate checked ingest path for temporary Parquet inputs. One SQL statement is accepted per call so planning sees the same catalog state as execution. Unsupported query forms fail closed. The gate also rejects forbidden `record_date` / `event_date` / `window_ts` references.
9. **Simplicity.** No ORM, no SQL AST framework, no `(year,month,day)` triples, no signal-specific clock aliases.

Violate any rule → reject the change.

---

## 1. Physical schema (one clock)

### 1.1 Disease

`record_date` + `timestamp`/`window_ts` taught “I filtered time.” Prune needed another column. Dual predicates were a bandage.

### 1.2 Law

Every DuckLake fact table has **`timestamp`** as its only time column. Partition = day(`timestamp`).

| Table | Timestamp type | Sort |
|-------|---------------|------|
| `traces` | `TIMESTAMP_NS` | `session_id, trace_id, timestamp` |
| `logs` | `TIMESTAMP_NS` | `session_id, trace_id, timestamp` |
| `scores` | `TIMESTAMP_NS` | `session_id, trace_id, timestamp` |

Score deduplication identity is `(score_id, timestamp)` in isolated scope and
`(tenant_id, score_id, timestamp)` in shared scope. A repeated `score_id` at a
different timestamp is a distinct score; every idempotency lookup carries the
score timestamp so it can prune to that day.

The one-time exporter rebuilds traces only. Existing logs and scores files stay
in place; their schemas and all subsequent writes/compaction follow the shared
profile. `score_configs` remains an auxiliary unpartitioned table.

**Locked partition expression** (greenfield EXPLAIN in `tests/integration/one_clock_prune.rs`):

```sql
ALTER TABLE … SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
```

`CAST(timestamp AS DATE)` is unsupported. `day(timestamp)` alone is day-of-month and collides across months — rejected. One EXPLAIN must show `timestamp` range opens only in-window day files. If prune fails, fix DDL/engine — **do not** add `record_date` back.

### 1.3 Query shape (only)

```sql
WHERE timestamp >= … AND timestamp <= … AND <identity>
```

Keep `timestamp` bare on the left side of each comparison. Do not cast or wrap
it: QueryWindow recipes and their tests construct both range bounds, EXPLAIN
verifies partition pruning, and the execute gate checks the pushed scan filters.
Loki/Tempo protocol times convert to the same UTC `QueryWindow` at the edge.

### 1.4 Cutover

Stop writers → export every physical scope → validate rows and Parquet metadata → flip once.

---

## 2. SQL architecture

### 2.1 Layers

```text
handler / planner  →  crate::sql::… recipe  →  TimestampFilteredSql  →  execute (+ D12 gate)
                         │
                    window.scan_with_timestamp_filter(|filter| …)
```

### 2.2 Module layout

```text
src/sql/
  literal.rs / ident.rs / bounds/{window,execute_gate}.rs
  schema/          # TableSpec + DDL
  llm/ compaction/ tempo/ session_summary/ writer/ promotion/ telemetry/
src/api/llm/query.rs         # HTTP only → sql::llm
```

**Locality test:** SQL verb string literals only under `src/sql/**` or in tests.

### 2.3 Contracts (input → output)

```rust
/// Sole lake time window. Protocol clocks → DateTime only at the edge.
pub struct QueryWindow { pub from: DateTime<Utc>, pub to: DateTime<Utc> }
pub struct TimestampFilteredSql { /* private */ sql: String }

impl QueryWindow {
    pub fn scan_with_timestamp_filter(self, alias: &str, assemble: impl FnOnce(&str) -> String) -> TimestampFilteredSql;
    /// Narrow to one calendar day as a *timestamp* sub-window (never emit DATE/day columns).
    pub fn scan_with_day_filter(self, day: NaiveDate, alias: &str, assemble: impl FnOnce(&str) -> String) -> TimestampFilteredSql;
}
```

`scan_with_timestamp_filter` / `scan_with_day_filter` emit bare `timestamp` lower/upper predicates and assert the assemble closure kept them. Never emit day/DATE predicates.

### 2.4 Enforcement

| Layer | Role |
|-------|------|
| `TimestampFilteredSql` + `scan_with_*_filter` | Injects the required filter and checks the assembled SQL retains that fragment; it does not parse SQL |
| `src/sql/` locality test | No SQL outside the package (tests excepted) |
| D12 execute gate + physical plan check | Reject fact scans without a pushed bare timestamp filter; reject forbidden time-column names |
| Greenfield EXPLAIN | Prove day prune from event-time filter |

### 2.5 Source-level timestamp lint (sketch)

This textual check gives an early error. Runtime enforcement uses DuckDB's
structured physical plan to check each scan and is wired to query, batch, and
prepared-statement execution paths.

```rust
pub fn ensure_sql_has_bare_timestamp_predicate(sql: &str) -> Result<(), String> {
    if is_allowlisted_infra(sql) { return Ok(()); }
    if sql.contains("record_date") || sql.contains("event_date") || sql.contains("window_ts") {
        return Err("forbidden time column name".into());
    }
    if names_fact_table(sql) && !has_bare_timestamp_predicate(sql) {
        return Err("fact-table SQL missing bare timestamp predicate".into());
    }
    Ok(())
}
```

Run the DuckDB physical-plan check in `DuckDBCore::execute_query_on_state`
when `query.sql_gate` is true (default), and always in `execute_batch_checked`,
`prepare_checked`, plus direct fact reads such as score existence checks.

---

## 3. Implementation plan

| Step | Change |
|------|--------|
| 1 | Greenfield catalog: §1 schema; lock partition DDL; EXPLAIN fixture |
| 2 | Create `src/sql/` (literal, bounds, gate, schema) |
| 3 | Move recipes into `src/sql/{…}`; callers SQL-free; locality test |
| 4 | `TimestampFilteredSql` + `scan_with_*_filter`; D12 gate |
| 5 | Export traces with the untracked `tmp/export_production_traces.py`; validate every scope before the coordinated flip |

---

## 4. Success criteria

- [x] No `record_date` in OTLP fact schemas (DDL)
- [x] EXPLAIN greenfield: day-of-`timestamp` prune (`tests/integration/one_clock_prune.rs`)
- [x] `src/sql/` foundation (`QueryWindow::scan_with_*_filter`, gate, literals, llm/tempo/session_summary recipes)
- [x] D12 source gate on checked write paths; physical-plan scan gate on `execute_query_on_state` when `query.sql_gate` is true (default), and on direct fact reads / writer paths always
- [ ] Production export validated across every physical scope before the coordinated cutover

Ops flip of catalogs remains an operator step after verify. Residual infra SQL still outside `src/sql/` (attach/DDL, Postgres dirty claim, OTLP telemetry compilers) — locality allowlist tracks the backlog; D12 gate covers execute paths.

---

## 5. Open questions

1. ~~Exact DuckLake day expression~~ — locked: `(year(timestamp), month(timestamp), day(timestamp))` on `TIMESTAMP_NS`.
2. Postgres `session_summary` stores event-time bounds as signed epoch nanoseconds (`start_time_ns` / `end_time_ns`). Convert to human-readable timestamps at the API boundary; do not round through Postgres `TIMESTAMPTZ`.

---

## Appendix A — Why the mess existed (context only)

Not the destination. Pre-redesign: ~66 files with SQL-ish strings, ~90 `compile_*`/`*_sql` fns, 9 duplicate escapers; collapse scanned on `window_ts` only while tables were `PARTITIONED BY (record_date)`. Fix is §0–§2, not dual predicates forever.
