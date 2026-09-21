# Tasks: remove-legacy-telemetry-sql-aliases

## 1. OpenSpec
- [x] 1.1 Scaffold proposal/tasks/spec deltas
- [x] 1.2 Validate with `openspec validate remove-legacy-telemetry-sql-aliases --strict`

## 2. Query engine
- [x] 2.1 Delete `rewrite_reserved_telemetry_view_names` and call site
- [x] 2.2 Collapse `ducklake_inline_sql` to `traces` / `logs` / `scores` only
- [x] 2.3 Delete `VIEW_COUNTERS` / view-recreate APIs; drop always-true `use_attached_catalog`

## 3. Gate and compat
- [x] 3.1 Remove `TELEMETRY_ALIASES` from execute_gate
- [x] 3.2 Drop `tm_*` missing-table swallows in DuckLake Loki/Tempo backends

## 4. Tests and docs
- [x] 4.1 Retarget/delete alias unit and perf tests; update Makefile / performance.yml
- [x] 4.2 Update live docs (`design.md`, `adhoc-duckdb-ducklake.md`, decision log, session-list summary)

## 5. Verify
- [x] 5.1 Scoped unit tests + `make test`; grep audit for leftover aliases
