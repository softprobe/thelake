# Trace event schema cutover

Trace event payloads are stored as DuckDB `JSON`. Their nested timestamps stay
RFC3339 JSON strings; only the top-level trace `timestamp` is the `TIMESTAMP_NS`
partition key.

DuckLake cannot alter the existing inline `LIST<STRUCT>` event column in place.
Before starting a release with the JSON writer, stop API instances and every
trace writer, then run the one-time table rebuild with the same DuckLake catalog
and warehouse configuration as the service:

```sh
CONFIG_FILE=/path/to/production-config.yaml cargo run --release --bin migrate_trace_events
```

The command reads inline catalog rows and active Parquet files, validates
session/trace/span identity for each source row, counts unmatched stale source
rows, and verifies every active event payload against the replacement before
swapping. It exits before the swap if source data cannot be converted or
validation fails. The old table is retained as `traces_legacy_<table-id>` until
API validation is complete; remove it only after that check succeeds.

Keep the service stopped until the command completes. If it stops between the
two table renames, rerunning the command restores the retained legacy table as
`traces`, removes the incomplete replacement, and exits with instructions to
rerun. If `traces` already uses JSON, the command is complete and leaves any
legacy table untouched for post-validation cleanup.

The replacement applies the global timestamp partition and session sort rules;
DuckLake's configured inlining policy determines whether each row stays in the
catalog or is written to Parquet. The cutover does not rewrite nested JSON
timestamps, logs, or score data. The test-only `scores` and `score_configs`
tables can be dropped separately and recreated by the new schema; score APIs
and code remain enabled.

Validate the API against the same catalog and warehouse before deployment:

```sh
python3 scripts/check_session_details.py \
  --base-url http://127.0.0.1:8080 \
  --from 2024-01-01T00:00:00Z \
  --to 2026-10-01T00:00:00Z \
  --inline-session <session-with-active-inline-rows-before-cutover> \
  --parquet-session <session-with-parquet-rows-before-cutover>
```

Set `THELAKE_API_TOKEN` for the local API. The check enumerates the entire
requested interval, loads every session detail (or recording-only payload), and
requires complete span/event arrays and matching span counts. It also requires
at least one event in each of the two known storage-path probes. This checks
that API loading works across the sweep and that both probes still expose
events; the sweep does not compare every response payload byte-for-byte with
its source. The migration performs that payload comparison before and after
the table swap. Use one session known to have had active inline rows in the
source snapshot and one known to have Parquet-backed rows; the rebuild may move
both into the new partitioned files.
