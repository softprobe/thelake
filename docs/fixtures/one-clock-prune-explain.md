# EXPLAIN fixture — one-clock greenfield prune

**Purpose:** Prove calendar-day partition of `timestamp` (no `record_date`) prunes on `timestamp` bounds only.

**Integration lock:** `tests/integration/one_clock_prune.rs` —
`greenfield_day_of_timestamp_partition_prunes_on_timestamp_bounds`

**Locked DDL:**

```sql
SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));
```

1. Create table with `timestamp TIMESTAMP_NS` only (no day columns).
2. Insert rows on 2026-01-10, 2026-02-10, 2026-09-10, 2026-09-11 (forces month separation).
3. Assert hive paths are `year=/month=/day=` (not `record_date=`).
4. Wide Sep 10–11 window: `Total Files Read: 2`.
5. Narrow Sep 10 window: `Total Files Read: 1`; day-B parquet stem absent from plan; row id = Sep-10 only.

**Rejected:** `day(timestamp)` alone (day-of-month collision); `CAST(timestamp AS DATE)` (unsupported).
