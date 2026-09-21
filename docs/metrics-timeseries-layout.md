# Metrics time-series layout (DuckLake) — one clock

**Status:** One-clock cutover (canonical with [`design-sql-and-schema.md`](./design-sql-and-schema.md))  
**Audience:** Implementation agents.

Related: [`design-sql-and-schema.md`](./design-sql-and-schema.md), [`design-event-time-layout.md`](./design-event-time-layout.md), [`goals.md`](goals.md), [`compat/phase1-prometheus.md`](compat/phase1-prometheus.md).

---

## 0. Law (non-negotiable)

1. **One event-time column named `timestamp` on every metrics fact table.**  
   Downsample / collapse bucket start is also `timestamp` (never `window_ts`).
2. **No `record_date` / `event_date` / `window_ts` columns.**
3. **Partition** = calendar day of `timestamp`:

   ```sql
   ALTER TABLE <t> SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
   ```

   Locked by `tests/integration/one_clock_prune.rs`. Bare `day(timestamp)` collides across months — rejected.
4. **Query shape:** `WHERE timestamp >= … AND timestamp <= …` (+ identity).  
   Prom `start_ms`/`end_ms` → `QueryWindow` at the protocol edge only.
5. **Production SQL** lives under `src/sql/` only. D12 `ensure_fact_scan_bound` rejects unbound fact scans and forbidden column names.
6. **Clean cutover:** new catalog → copy → flip → delete old. Script: [`scripts/one_clock_catalog_copy.sql`](../scripts/one_clock_catalog_copy.sql). No dual-read / feature flags.

Violate any rule → reject the change.

---

## 1. Problem (why one clock)

The dual-clock disease: a DATE partition column (`record_date`) plus a separate event clock taught “I filtered time” while prune needed another predicate. `window_ts` made downsample look like a second religion. Dual predicates were a bandage.

One clock: the event-time filter **is** the prune.

---

## 2. Goals (unchanged intent)

| Id | Intent |
|----|--------|
| G1 | DuckLake is the only store for metrics identity, samples, histograms, downsamples, collapse |
| G2 | Fast interactive Prom/Grafana queries on a release binary with ingest running |
| G3 | Partition prune + sort locality without cross-day merge theater |
| G4 | Skinny samples; postings resolve before sample scan |
| G5 | Ladder (5m → 1h → collapse) for long windows |

Perf gate (release verification) remains:

```bash
CARGO_PROFILE_FLAG=--release PERF_SUITE=metrics-layout \
  METRICS_LAYOUT_PROFILE=release_full COMPARE_GREPTIME=1 \
  make test-perf
```

---

## 3. Physical schema

### 3.1 Tables

| Table | Role | Sort lead |
|-------|------|-----------|
| `metric_series` | series identity + labels | `series_id, timestamp` |
| `metric_postings` | label → series_id | `series_id, timestamp` |
| `metric_samples` | gauge/sum/counter samples | `series_id, timestamp` |
| `metric_hist_samples` | histogram fidelity | `series_id, timestamp` |
| `metric_samples_5m` / `_1h` | scalar downsample | `series_id, timestamp` |
| `metric_hist_samples_5m` / `_1h` | hist downsample | `series_id, timestamp` |
| `metric_collapse_job_1h` | job collapse | `metric_name, job, timestamp` |

Every table above is **partitioned by** `(year(timestamp), month(timestamp), day(timestamp))`.

Hive paths look like `year=2026/month=9/day=10/…parquet` — never `record_date=…`.

### 3.2 Column notes

- **`timestamp`**: event time. On downsample/collapse rows this is the **bucket start**.
- **Joins**: `series_id` (+ time bounds). Do **not** join on a day column.
- **Maintenance day scoping**: `CAST(timestamp AS DATE) = DATE '…'` or `QueryWindow::bind_day` inside `src/sql/` only — never emit a stored day column.

### 3.3 DDL sketch

```sql
CREATE TABLE metric_samples (
  series_id UBIGINT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  value DOUBLE
);
ALTER TABLE metric_samples
  SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
ALTER TABLE metric_samples
  SET SORTED BY (series_id, timestamp);

CREATE TABLE metric_samples_5m (
  series_id UBIGINT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,  -- bucket start
  count UBIGINT, sum DOUBLE, min DOUBLE, max DOUBLE, last DOUBLE, last_ts TIMESTAMPTZ
);
-- same PARTITIONED BY / SORTED BY pattern
```

Registry / partition helpers: `src/sql/schema` (`ONE_CLOCK_PARTITION_BY`, `TableSpec`).

---

## 4. Query path

```text
Prom edge (ms) → QueryWindow → src/sql/prom recipes → BoundLakeSql → execute (+ D12)
```

1. Equality matchers → postings intersect (`src/sql/prom`) with **timestamp** day coverage.
2. Skinny sample scan on chosen grain (`raw` / `5m` / `1h` / hist) with `timestamp` bounds only.
3. Collapse shortcut when AST + window qualify (`src/sql/compaction` collapse scan).

Forbidden: `record_date` / `window_ts` tokens in emitted SQL (gate rejects).

---

## 5. Compaction / TWCS / ladder

- **TWCS window** = calendar day of `timestamp` (reconstructed from year/month/day partition keys in DuckLake metadata).
- Softprobe plans **one merge intent per day**; never cross-day rewrite theater.
- Ladder INSERTs are pending-day + per-day scoped so partitions prune; lag uses `timestamp < now() - lag`.
- SQL recipes: `src/sql/compaction/{downsample,collapse}.rs`. Planner/executor call those — no embedded SQL verbs.

---

## 6. Cutover

1. Create **new** empty catalog with one-clock DDL.
2. Prove prune: `tests/integration/one_clock_prune.rs` green.
3. Copy once: [`scripts/one_clock_catalog_copy.sql`](../scripts/one_clock_catalog_copy.sql)  
   (`EXCLUDE (record_date)`; rename `window_ts` → `timestamp`).
4. Flip config to new catalog; delete old.
5. No dual-read era.

Ops flip remains an operator step after verify.

---

## 7. Acceptance mapping (legacy AC ids)

Historical AC-\* ids in perf harnesses still apply with **one-clock semantics**:

| Legacy wording | One-clock meaning |
|----------------|-------------------|
| `record_date` column / partition | Calendar day of `timestamp` / hive `year`/`month`/`day` |
| `window_ts` | `timestamp` (bucket start) |
| `PARTITIONED BY (record_date)` | `PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp))` |
| Dual `record_date` + timestamp predicates | **Forbidden** — timestamp bounds only |

Evidence JSON paths under `docs/perf/results/` remain the release gate artifacts; interpret column names through the mapping above.

---

## 8. Out of scope

- Dual-read / emit-both forever
- Reintroducing `record_date` if prune fails (fix DDL/engine instead)
- Postgres `session_summary.start_time` redesign (side store)
- PromQL grammar changes
