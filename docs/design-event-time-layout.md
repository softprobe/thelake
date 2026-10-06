# Event-time layout

This document defines the physical layout and query-time rules for DuckLake
`traces`, `logs`, and `scores`. SQL and schema ownership are described in
[`design-sql-and-schema.md`](design-sql-and-schema.md).

## Timestamp contract

- Fact tables use one event-time column named `timestamp`, stored as
  `TIMESTAMP_NS` and interpreted as a UTC instant.
- Every fact query requires a bounded `QueryWindow`. SQL uses bare `timestamp`
  predicates so DuckLake can prune partitions.
- Partition keys are calendar year, month, and day derived from `timestamp`.
- Queries and schemas do not use `record_date`, `event_date`, or `window_ts`.
- SQL compilers live under `src/sql/`; handlers use those compilers.

The query-worker SQL gate can inspect DuckDB's physical plan when
`query.sql_gate` is enabled. Writer and score-lookup paths retain their own
execution checks.

## Physical profile

All three fact tables use the same profile:

| Property | Value |
|---|---|
| Partition expression | `year(timestamp), month(timestamp), day(timestamp)` |
| Sort order | `(session_id, trace_id, timestamp)` |
| Row-group target | 8 MiB |
| File target | 128 MiB |

These are size targets. Sorting can improve statistics within row groups, but
row groups may contain multiple session IDs. A session that crosses a UTC day
boundary spans more than one partition.

## Query windows

Typed query APIs require `QueryWindow { from, to }`. Compilers use
`QueryWindow::scan_with_timestamp_filter` to put explicit bounds on each fact
scan. `scan_with_day_filter` may narrow a timestamp window contained within one
calendar day; it does not use a date column.

Session detail reads use the start and end stored in `session_summary`. Search
and trace lookup APIs require request time bounds. There is no unbounded default
scan over fact tables.

## Pruning proof

The locked layout check is
`tests/integration/one_clock_prune.rs::production_writers_partition_and_prune_one_clock_fact_tables`.
It writes through production writers and verifies that a one-day query reads
only the matching day's files. The companion fixture is
[`fixtures/one-clock-prune-explain.md`](fixtures/one-clock-prune-explain.md).

The related query compiler check is
`one_day_session_fetch_predicates_do_not_name_unrelated_days` in
`src/api/llm/query.rs`.
