# Event-time layout for OTLP tables (traces / logs / scores)

**Status:** Design — **one-clock clean cutover** (aligned with [`design-sql-and-schema.md`](./design-sql-and-schema.md)). Prior “keep `record_date` + dual predicates” plan **superseded**.  
**Scope:** DuckLake `traces`, `logs`, `scores`  
**Related:** [`design-sql-and-schema.md`](./design-sql-and-schema.md), [#73](https://github.com/softprobe/thelake/issues/73)

**Constraints:** (1) simplicity (2) clean cutover — one-time copy OK, no compat (3) no room for mistake

---

## 0. Non-negotiable rules

1. **One event time.** Column `timestamp` is the only temporal column. Type: `TIMESTAMP_NS` (unified).
2. **No `record_date` / `event_date` / `window_ts` column.** Partition = calendar day of `timestamp` (DuckLake expression locked by greenfield EXPLAIN). Filtering `timestamp` **is** the prune.
3. **Session locality is sort, not partition.** Never `PARTITIONED BY (session_id)`.
4. **Every OTLP read requires `QueryWindow { from, to }`.** No `Option` time. Compilers emit **only** `timestamp` bounds via `QueryWindow::bind_*` in `src/sql/` (`bind_scan` for a window; `bind_day` only as a `timestamp` sub-window for one calendar day — never a DATE/day-column predicate).
5. **All OTLP SQL lives in `src/sql/`.** Handlers call `crate::sql::…`.
6. **Execute-time gate (D12)** rejects fact SQL without a `timestamp` bound and rejects `record_date` / `event_date` / `window_ts`.
7. **No backwards compatibility.** New catalog → copy → flip → delete old.

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
| D3 | **Cutover:** new catalog; batch copy; flip; drop old. No dual-read. |
| D4 | **Delete** `push_optional_time_bounds` and any optional lake windows. |
| D5 | **One type:** `TIMESTAMP_NS` on traces/logs/scores. |
| D6 | **Session detail window** = summary start/end only. Pad = **0**. |
| D7 | **Sort:** traces `(session_id, trace_id, timestamp)`; logs/scores `(session_id, timestamp)`. |
| D8 | **No `app_id` sort lead.** Scores in same layout module. |
| D9 | **Execute gate + `src/sql` locality** — see sql/schema design. |
| D10 | **Inline** default `data_inlining_row_limit = 500`. |
| D11 | Session `/observations` include `attributes`/`events` for Explorer trajectory. |

**Pre-cutover (once):** greenfield EXPLAIN with **only** `timestamp` bounds must not read out-of-window day files. If prune fails, fix DDL/engine — do **not** reintroduce `record_date`.

---

## 3. Target layout

```sql
-- columns: … payload …, timestamp TIMESTAMP_NS NOT NULL
-- no record_date
ALTER TABLE traces SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
ALTER TABLE traces SET SORTED BY (session_id, trace_id, timestamp);
```

```sql
WHERE timestamp >= … AND timestamp <= … AND <identity>
```

Locked by `tests/integration/one_clock_prune.rs` / [`fixtures/one-clock-prune-explain.md`](./fixtures/one-clock-prune-explain.md).
---

## 4. API / product

| Endpoint | Window |
|----------|--------|
| Session detail / observations / recording | `session_summary` start/end only |
| Search | Request `from`/`to` required |
| Trace / observation by id | Require window; 400 if missing |

---

## 5. Acceptance

1. No `record_date` in OTLP schemas.  
2. Every OTLP recipe: `timestamp` bounds only; no `record_date` / `event_date` / `window_ts` tokens.  
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
- Relying on review instead of `src/sql` + bind + gate.  
- Reintroducing `record_date` because triples feel complex.

**This document and [`design-sql-and-schema.md`](./design-sql-and-schema.md) are the law.** Prior DATE-column + dual-predicate revisions are obsolete.
