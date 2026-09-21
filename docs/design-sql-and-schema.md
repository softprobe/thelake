# Design: SQL compilation + one-clock schema

**Status:** Implementation in progress — one-clock foundation + schema DDL + collapse SQL; recipe migration continuing  
**Constraints:** (1) simplicity (2) clean cutover — one-time copy OK, no compat (3) no room for mistake  
**Related:** [`design-event-time-layout.md`](./design-event-time-layout.md), [`metrics-timeseries-layout.md`](./metrics-timeseries-layout.md) (override banner)

---

## 0. Non-negotiable rules

1. **One event-time column named `timestamp` on every DuckLake fact table.**  
   That filter is the prune. Physical layout = calendar day of `timestamp`.  
   **No `record_date` / `event_date` / `window_ts`.** (Downsample bucket start is still `timestamp` — the row’s event time at that grain.)
   The physical timestamp type is intentionally family-specific: metrics layout
   tables use `TIMESTAMPTZ` because Prometheus/OTLP metric query recipes and
   zone-map literals use an absolute timezone-bearing clock; OTLP `traces` and
   `logs` use `TIMESTAMP_NS` because their public contracts preserve Unix
   nanoseconds. Both are the same logical one-clock column, and every recipe
   binds the column named `timestamp`; the type distinction is not a second
   window or partition key.
2. **One window type: `QueryWindow { from, to }`.** Recipes call `bind_scan` on it. Prom/Tempo ms clocks convert at the protocol edge only — not a second lake window type.
3. **All production SQL under `src/sql/` only.** Tests are the only exception. Locality unit test is hard-fail.
4. **Handlers / planners / writers never embed SQL verbs.** They call `crate::sql::…`.
5. **One escaping / quoting API** under `src/sql/`.
6. **Schema registry in `src/sql/schema` (types + DDL).**
7. **Clean cutover.** New catalog, copy once, flip, drop old. No dual-read / feature flags.
8. **Execute-time gate (D12):** fact-table SQL missing a `timestamp` bound → reject; any `record_date` / `event_date` / `window_ts` token → reject.
9. **Simplicity.** No ORM, no SQL AST framework, no `(year,month,day)` triples, no metrics-only clocks or column aliases.

Violate any rule → reject the change.

---

## 1. Physical schema (one clock)

### 1.1 Disease

`record_date` + `timestamp`/`window_ts` taught “I filtered time.” Prune needed another column. Separate `MetricsWindow` / `window_ts` made metrics look special. Dual predicates were a bandage.

### 1.2 Law

Every DuckLake fact table has **`timestamp`** as its only time column. Partition = day(`timestamp`). Metrics are not a second religion.

| Table family | Sort (lead) |
|--------------|-------------|
| `traces` | `session_id, trace_id, timestamp` |
| `logs`, `scores` | `session_id, timestamp` |
| `metric_*` samples / series / postings / hist | `series_id, timestamp` |
| downsample / collapse (`metric_samples_5m/1h`, …) | grain keys + `timestamp` (bucket start; was `window_ts`) |

**Locked partition expression** (greenfield EXPLAIN in `tests/integration/one_clock_prune.rs`):

```sql
ALTER TABLE … SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
```

`CAST(timestamp AS DATE)` is unsupported. `day(timestamp)` alone is day-of-month and collides across months — rejected. One EXPLAIN must show `timestamp` range opens only in-window day files. If prune fails, fix DDL/engine — **do not** add `record_date` back.

### 1.3 Query shape (only)

```sql
WHERE timestamp >= … AND timestamp <= … AND <identity>
```

Prom: `start_ms`/`end_ms` → `QueryWindow` at the edge, then the same shape.

### 1.4 Metric index identity

`metric_series` and `metric_postings` are day-scoped index facts, not one-time
dimension tables and not sample-frequency event tables. Their deduplication
identity is therefore:

- `metric_series`: `(series_id, UTC calendar day)`
- `metric_postings`: `(label_name, label_value, series_id, UTC calendar day)`

The writer retains one representative row for each identity per day. This
keeps persistent series visible to Prometheus postings and series-metadata
queries for every day in which they are observed, while avoiding one index row
per exact sample timestamp. The day is derived from the single `timestamp`
column with an explicit UTC `date_trunc('day', timestamp AT TIME ZONE 'UTC')`;
no second date column is introduced. Any join from samples or postings to these
day-scoped index facts must include the same UTC-day predicate, otherwise a
persistent series would multiply rows across retained days.

### 1.5 Cutover

New catalog → EXPLAIN → copy (drop `record_date`; rename `window_ts` → `timestamp`) → flip → delete old.

---

## 2. SQL architecture

### 2.1 Layers

```text
handler / planner  →  crate::sql::… recipe  →  BoundLakeSql  →  execute (+ D12 gate)
                         │
                    window.bind_scan(|bound| …)
```

### 2.2 Module layout

```text
src/sql/
  literal.rs / ident.rs / bounds/{window,execute_gate}.rs   # one window module, not otlp vs metrics
  schema/          # TableSpec + DDL
  llm/ prom/ compaction/ tempo/ session_summary/ writer/ promotion/
src/compaction/collapse.rs   # planner only → sql::compaction
src/api/llm/query.rs         # HTTP only → sql::llm
```

**Locality test:** SQL verb string literals only under `src/sql/**` or in tests.

### 2.3 Contracts (input → output)

```rust
/// Sole lake time window. Prom ms → DateTime only at the Prom/Tempo edge.
pub struct QueryWindow { pub from: DateTime<Utc>, pub to: DateTime<Utc> }
pub struct BoundLakeSql { /* private */ sql: String }

impl QueryWindow {
    pub fn bind_scan(self, alias: &str, assemble: impl FnOnce(&str) -> String) -> BoundLakeSql;
    /// Narrow to one calendar day as a *timestamp* sub-window (never emit DATE/day columns).
    pub fn bind_day(self, day: NaiveDate, alias: &str, assemble: impl FnOnce(&str) -> String) -> BoundLakeSql;
}
```

`bind_scan` / `bind_day` emit `timestamp` lower/upper only and assert the assemble closure kept them. Never emit day/DATE predicates.

### 2.4 Example — collapse scan (target)

```rust
pub fn collapse_scan_sql(catalog: &str, metric: &str, window: QueryWindow, limit: usize)
    -> BoundLakeSql
{
    let table = TABLES.metric_collapse_job_1h.qualified(catalog);
    let name = sql_string_literal(metric);
    window.bind_scan("c.", |bound| {
        format!(
            "SELECT … FROM {table} c WHERE c.metric_name = {name} AND {bound} \
             ORDER BY c.job, c.timestamp LIMIT {limit}"
        )
    })
}
```

### 2.5 Enforcement

| Layer | Role |
|-------|------|
| `BoundLakeSql` + `bind_*` | Cannot omit event-time fragment |
| `src/sql/` locality test | No SQL outside the package (tests excepted) |
| D12 execute gate | Reject missing event-time bound; reject `record_date` |
| Greenfield EXPLAIN | Prove day prune from event-time filter |

### 2.6 D12 gate (sketch)

```rust
pub fn ensure_fact_scan_bound(sql: &str) -> Result<(), String> {
    if is_allowlisted_infra(sql) { return Ok(()); }
    if sql.contains("record_date") || sql.contains("event_date") || sql.contains("window_ts") {
        return Err("forbidden time column name".into());
    }
    if names_fact_table(sql) && !has_timestamp_bound(sql) {
        return Err("fact-table SQL missing timestamp bound".into());
    }
    Ok(())
}
```

Wire into `DuckDBCore::execute_query_on_state` and shared `execute_batch_checked` for compaction/writer.

---

## 3. Implementation plan

| Step | Change |
|------|--------|
| 1 | Greenfield catalog: §1 schema; lock partition DDL; EXPLAIN fixture |
| 2 | Create `src/sql/` (literal, bounds, gate, schema) |
| 3 | Move recipes into `src/sql/{…}`; callers SQL-free; locality test |
| 4 | `BoundLakeSql` + `bind_*`; D12 gate |
| 5 | Copy → flip → delete old |
| 6 | Rewrite `metrics-timeseries-layout.md` |

---

## 4. Success criteria

- [x] No `record_date` in OTLP/metrics fact schemas (DDL)  
- [x] EXPLAIN greenfield: day-of-`timestamp` prune (`tests/integration/one_clock_prune.rs`)  
- [x] `src/sql/` foundation (`QueryWindow::bind_*`, gate, literals, collapse/downsample/prom/llm/tempo/session_summary recipes)  
- [x] D12 gate on `execute_query_on_state` + `execute_batch_checked` on compaction ladder + writer insert path  
- [x] Recipes under `src/sql/` for compaction / prom / llm / tempo / session_summary list+reduce  
- [x] Cutover script: [`scripts/one_clock_catalog_copy.sql`](../scripts/one_clock_catalog_copy.sql)  
- [x] [`metrics-timeseries-layout.md`](./metrics-timeseries-layout.md) rewritten to one-clock law  

Ops flip of catalogs remains an operator step after verify. Residual infra SQL still outside `src/sql/` (attach/DDL, TWCS metadata probes, Postgres dirty claim, OTLP telemetry compilers, metrics writer INSERT builders) — locality allowlist tracks the backlog; D12 gate covers execute paths.

---

## 5. Open questions

1. ~~Exact DuckLake day expression~~ — locked: `(year(timestamp), month(timestamp), day(timestamp))` on `TIMESTAMP_NS`.  
2. Postgres `session_summary` keeps `start_time` (not DuckLake) — confirm out of one-clock lake law.

---

## Appendix A — Why the mess existed (context only)

Not the destination. Pre-redesign: ~66 files with SQL-ish strings, ~90 `compile_*`/`*_sql` fns, 9 duplicate escapers; collapse scanned on `window_ts` only while tables were `PARTITIONED BY (record_date)`. Fix is §0–§2, not dual predicates forever.
