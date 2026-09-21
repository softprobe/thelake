# Change: remove-legacy-telemetry-sql-aliases

## Why

The Iceberg/buffer-era query path rewrote `union_*`, `committed_*`, `buffer_*`,
`staged_*`, and `iceberg_*` names through intermediate `tm_*` tokens before
qualifying DuckLake tables. Those TEMP VIEWs and multi-tier physical stores are
gone. First-party compilers already emit `traces` / `logs` / `scores`. The
rewrite shim, orphan `tm_*` acceptors, and dead view-recreate counters are
leftover complexity with no product value.

## What Changes

- Delete `rewrite_reserved_telemetry_view_names` and all legacy public alias
  mappings (`union_*`, `committed_*`, `buffer_*`, `staged_*`, `iceberg_*`).
- Collapse `ducklake_inline_sql` to qualify only bare `traces` / `logs` /
  `scores` against the attached DuckLake catalog.
- Remove dead `VIEW_COUNTERS` / view-recreate snapshot APIs.
- Drop legacy names from the D12 fact-scan gate alias list and from Loki/Tempo
  missing-table empty-result heuristics.
- Update live docs and tests; leave `docs/legacy/` as historical archive.

## Non-goals

- Product metrics / `union_metrics` / Prometheus removal (see
  `remove-metrics-signal`).
- Merging the two SQL string lexers in query vs execute-gate.
- Changing `make duckdb-shell` convenience views for `traces` / `logs`.

## Impact

- Breaking: `POST /v1/query/sql` no longer rewrites Iceberg/buffer aliases;
  clients must use `traces` / `logs` / `scores`.
- Query engine, D12 gate, Tempo/Loki empty-table heuristics, perf tests, docs.
