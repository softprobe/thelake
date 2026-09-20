# EXPLAIN fixture — one-day session fetch (AC6)

**Purpose:** Prove D4 predicates (`record_date BETWEEN` + event-time bounds) are the prune shape for a one-day session lake fetch. Unrelated calendar days must not appear in the day predicate.

**Canonical compile shape** (from `compile_session_observations_sql` / `QueryWindow`):

```sql
-- window: 2026-09-10T16:05:15Z .. 2026-09-10T16:45:48Z
... FROM traces WHERE
  record_date BETWEEN DATE '2026-09-10' AND DATE '2026-09-10'
  AND session_id = '...'
  AND CAST(timestamp AS TIMESTAMP_NS) >= '2026-09-10T16:05:15Z'::TIMESTAMP_NS
  AND CAST(timestamp AS TIMESTAMP_NS) <= '2026-09-10T16:45:48Z'::TIMESTAMP_NS
```

**Pass criteria:**

1. Day predicate lists **only** `2026-09-10` (not `2026-09-09` / `2026-09-11`).
2. Event-time bounds match the same window (pad 0 for session detail).
3. Unit lock: `one_day_session_fetch_predicates_do_not_name_unrelated_days` in `api/llm/query.rs`.

**Out of scope here:** DuckDB physical “Files Read” text varies by catalog; the compile inventory is the durable gate. Re-run a live `EXPLAIN` against a reset catalog when evaluating `(year,month,day)(timestamp)` transforms (design §2 blocking experiment).
