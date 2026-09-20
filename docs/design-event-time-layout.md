# Event-time layout for OTLP tables (traces / logs / scores)

**Status:** Design — revised after hostile review (prior M0–M5 / `(year,month,day)` plan **rejected**)  
**Scope:** DuckLake `traces`, `logs`, `scores`  
**Out of scope:** metrics physical redesign (must still obey §0 rule 1 when touched; no permanent second religion — track a follow-up with a kill date, do not carve out forever)  
**Related:** [GitHub #73](https://github.com/softprobe/thelake/issues/73), [`session-list-summary.md`](./session-list-summary.md), [`design-ducklake-42-43.md`](./design-ducklake-42-43.md), public narrative [`blog/always-time-bound-ai-agent-trace-lakes.md`](./blog/always-time-bound-ai-agent-trace-lakes.md)

---

## 0. Non-negotiable rules

1. **One event time.** Column `timestamp` is the only temporal truth (OTLP start / log time / score time).
2. **Partition day is `date(timestamp)` — nothing else.** Never a second caller-supplied clock. Never independently settable on a struct.
3. **Session locality is sort, not partition.** Forbidden: `PARTITIONED BY (session_id)` or `bucket(session_id)` as the time strategy.
4. **Every OTLP DuckLake read requires a finite event-time window.** Missing / optional / half-bounds are compile errors.
5. **One way to build time SQL.** One required helper. No `push_optional_time_bounds` for table scans.
6. **Simplicity.** Two names for the same instant, two partition shapes, dual-read periods, and “optional if EXPLAIN” predicates are bugs.

Violate any rule → reject the change.

---

## 1. Disease

We stored one fact as two objects (`timestamp` + `record_date`) and let subsystems pick different ones:

- Writers: keep them aligned.
- Reducer: **both** `record_date` + `CAST(timestamp AS TIMESTAMP_NS)` ([`reduce_sql.rs`](../src/session_summary/reduce_sql.rs), [`session-list-summary.md`](./session-list-summary.md)).
- LLM / Tempo / Loki: timestamp only, sometimes optional ([`sql_support.rs`](../src/api/sql_support.rs) `push_optional_time_bounds`).
- Layout today: `PARTITIONED BY (record_date)`, sort leads with `app_id` on traces; **scores not in** [`otlp_layout.rs`](../src/storage/schema/otlp_layout.rs).
- Explorer: list `range=30d` becomes detail lake window.

Engineers think “time-bounded” ⇒ “partition-pruned.” The second name taught that lie.

This is a **correctness / mental-model** failure. Latency on session detail is a symptom (also fat attrs + large inline).

---

## 2. Decisions (hard — no open research in the adopted plan)

| # | Decision |
|---|----------|
| D1 | **Keep physical partition key as a DATE column** named only as the storage of `date(timestamp)`. Do **not** switch to `PARTITIONED BY (year,month,day)(timestamp)` until a blocking EXPLAIN fixture proves equal-or-better prune **and** we accept day-of-month triple complexity. Default: **keep DATE identity partition**. |
| D2 | That DATE column may stay named `record_date` in Parquet **only as legacy spelling** of `date(timestamp)`. In **all new code and docs**, call it **partition day derived from event time**, never a second clock. Prefer renaming to `event_date` in the next clean catalog; until then, **one writer function** sets it: `event_date = timestamp.date_naive()`. |
| D3 | **Callers never pass partition day.** Only `QueryWindow { from, to }`. |
| D4 | Helper always emits: (a) `timestamp` bounds, (b) `event_date BETWEEN date(from) AND date(to)` derived from the same window. Not optional. Not “if EXPLAIN.” One shape forever for OTLP compilers (including reduce). |
| D5 | **Delete** `push_optional_time_bounds` for OTLP scans. No `Option<from/to>` on session/trace lake compilers. |
| D6 | **Unify filter type:** stop emitting `CAST(timestamp AS TIMESTAMP_NS)` once the table type is fixed; one physical timestamp type per table. Until unified, helper owns the single cast form — callers never invent casts. |
| D7 | **Session detail window** = `session_summary.start_time .. end_time` only. Pad = **0**. Client list range is ignored for lake scan. No clamp / widen / intersect policy. |
| D8 | **Sort:** `traces`: `(session_id, trace_id, timestamp)`; `logs`: `(session_id, timestamp)`; `scores`: `(session_id, timestamp)` and **reject writes** missing `session_id` and `trace_id` both empty for sort purposes (scores already require at least one of session/trace/span — extend layout: if `session_id` present sort leads with it, else `trace_id`, else `span_id` — **one** deterministic order function in writer, not two documented religions). |
| D9 | **Drop `app_id` from OTLP sort lead** (tenant is catalog-scoped). Align writer `ORDER BY` with `SET SORTED BY`. |
| D10 | **Put `scores` in the same layout module** as traces/logs (`PARTITIONED BY` day-of-timestamp, `SET SORTED BY` as above). |
| D11 | **No dual-read era. No feature flags. No M0–M5 staircase.** Ship: helper + required bounds + layout/sort alignment + doc hard-rule rewrite in one change series. Catalogs that cannot evolve: **reset**. |
| D12 | **No execute-time SQL regex guard.** Enforcement = typed required window at compile APIs + unit tests on every `compile_*`. Ban new optional-bound call sites by deleting the API. |
| D13 | **Inline:** keep default `data_inlining_row_limit = 500`; treat larger inline as a layout regression. |
| D14 | **Projection:** session observation list path must not select full `attributes`/`events` in the same effort that claims session fetch is fixed (separate PR OK, same milestone). |

**Blocking experiment before any partition-transform migration:** EXPLAIN on current `PARTITIONED BY (record_date)` with helper shape D4 — prove Files Read. Only then consider dropping the DATE column for timestamp transforms. Until that experiment is attached to a verification report, **transforms are out of scope**.

---

## 3. Target layout (what we ship)

### 3.1 Partition

```sql
ALTER TABLE traces SET PARTITIONED BY (record_date);  -- record_date := date(timestamp) only
ALTER TABLE logs   SET PARTITIONED BY (record_date);
ALTER TABLE scores SET PARTITIONED BY (record_date);
```

Writer:

```text
record_date = timestamp.date_naive()   // single function; no other assignment site
```

### 3.2 Sort

```sql
ALTER TABLE traces SET SORTED BY (session_id, trace_id, timestamp);
ALTER TABLE logs   SET SORTED BY (session_id, timestamp);
ALTER TABLE scores SET SORTED BY (session_id, timestamp);  -- see D8 for null session_id
```

### 3.3 Query SQL shape (only allowed)

```sql
WHERE record_date BETWEEN DATE '…' AND DATE '…'   -- derived from from/to
  AND timestamp >= … AND timestamp <= …           -- same window
  AND <identity predicates>
```

Predicate order: partition day → identity → timestamp (matches reduce today; keep).

### 3.4 Forbidden

- `push_optional_time_bounds`
- Client-supplied `record_date` / partition params
- Unbounded id lookup (`WHERE span_id = ?` alone)
- `PARTITIONED BY (session_id)`
- Dual-read / “support both layouts”
- `PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp))` until D1 experiment passes
- Execute-layer SQL grepping as the primary safety net

---

## 4. API / product

| Endpoint | Window |
|----------|--------|
| `GET /v1/llm/sessions/{id}` | From `session_summary` only (D7) |
| `GET /v1/llm/sessions/{id}/observations` | Same |
| `GET /v1/llm/sessions/{id}/recording` | Same |
| Search endpoints | Request `from`/`to` required; helper D4 |
| Trace / observation by id | Require window (from summary or explicit); 400 if missing |

Explorer stops passing list `range` into detail lake calls.

---

## 5. Doc / code rewrites in the same series

| Artifact | Change |
|----------|--------|
| This doc | Source of truth |
| [`session-list-summary.md`](./session-list-summary.md) | Hard rule → **QueryWindow + helper D4** (not “remember two independent columns”) |
| [`otlp_layout.rs`](../src/storage/schema/otlp_layout.rs) | Sort keys D8/D9; include scores |
| [`sql_support.rs`](../src/api/sql_support.rs) | Required helper; delete optional |
| [`reduce_sql.rs`](../src/session_summary/reduce_sql.rs) | Call the same helper |
| All `compile_*` in `api/llm/query.rs`, telemetry, Tempo, Loki | Required window |
| Tempo / Loki scan adapters | Omitted client bounds → finite default lookback (still a `QueryWindow`; never unbounded) |
| Issue #73 | Retitle: required event-time window + single day-from-timestamp; fail closed at compilers |

---

## 6. Acceptance

1. One writer function assigns partition day from `timestamp`; tests fail if any other assignment exists.
2. Every OTLP `compile_*` test asserts both derived day bounds and timestamp bounds; none accept `Option` time.
3. `push_optional_time_bounds` deleted or unusable for OTLP tables.
4. Session detail ignores list range; uses summary start/end only.
5. `otlp_layout` covers traces, logs, scores; writer `ORDER BY` matches `SORTED BY`.
6. EXPLAIN fixture committed: session fetch for a one-day session does not list unrelated day files when D4 predicates are present.
7. [`session-list-summary.md`](./session-list-summary.md) hard rule rewritten to point here — no contradictory “two independent duties.”
8. No dual-read period in the rollout notes.

---

## 7. Explicitly rejected (hostile review)

- Six-phase migration / dual-read / transitional “may emit record_date.”
- Partition by `(year, month, day)(timestamp)` as the default (day-of-month triple; unproven prune).
- Optional `push_derived_calendar_day_bounds` gated on EXPLAIN.
- Execute-time SQL regex allowlist as primary enforcement.
- Client clamp/intersect with summary bounds.
- Speculative `hour()` partitions.
- Eternal metrics carve-out without a follow-up kill date.
- Module name TBD / open pad questions — decided: pad **0**.

---

## 8. What we keep from the original diagnosis

- Dual naming taught the wrong prune algebra — fix the **query contract** and **single assignment** of day-from-timestamp.
- Do not hive-partition by session.
- Required finite event-time windows everywhere.
- Session detail must not inherit list `range=30d`.
- Sort for session locality.

**Adopt the rules and §2–§3. Do not adopt the rejected transform + phased dual-model plan.**
