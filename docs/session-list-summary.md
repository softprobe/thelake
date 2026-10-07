# Session summaries

The session list reads a compact summary table in catalog PostgreSQL. Session
details and all retained evidence continue to come from DuckLake `traces`.

```text
successful trace commit -> batched dirty-session upsert
dirty-row reducer       -> claim rows -> aggregate traces -> upsert summary
session list            <- session_summary
session detail          <- traces
```

The summary is derived data. It can be rebuilt from traces, and a summary
failure does not fail an ingest request. Every DuckLake fact scan uses a finite
time window with a bare `timestamp` predicate so partitions can be pruned.

## PostgreSQL tables

`session_summary` is stored in the tenant metadata schema and contains the
list fields: session ID, start and end timestamps, observation and error
counts, token totals, cost, and promoted filter fields such as agent and user.
It does not store span payloads, prompts, attributes, or events.

`session_summary_dirty` stores one row per session needing reduction, including
the dirty time bounds and claim ownership. A generation value protects a newer
dirty update from being deleted by an older reducer attempt. Reducers claim
batches with `FOR UPDATE SKIP LOCKED`; expired claims can be retried by another
replica.

## Updates and rebuilds

After a successful DuckLake trace commit, ingest writes one batched dirty
upsert for the distinct session IDs in that commit. It does not write dirty
state per span. Failed lake commits do not mark sessions dirty.

The reducer aggregates claimed sessions from `traces` within the dirty time
bounds, updates `session_summary`, then acknowledges the matching dirty-row
generations. Late spans mark the session dirty again and are included in a
later reduction.

The leased rebuild job uses the same aggregate and an explicit `{from,to}`
window. The operator endpoint is `POST /v1/sessions/summary/rebuild`.
Periodic rebuilds use `rebuild_interval_ms` and
`max_reduce_span_seconds` from `session_summary` configuration. Rebuilds do
not scan the full lake.

## Read behavior

- `POST /v1/sessions/search` reads summary rows with cursor pagination. It
  does not scan `traces` or fall back to a lake aggregation.
- Session detail reads full span data and aggregates from DuckLake `traces`.
- Session recording uses its separate recording endpoint.
- Explorer reads summaries and never writes the summary tables.

## Configuration

```yaml
ingest:
  flush_interval_seconds: 0
session_summary:
  reducer_interval_ms: 10000
  rebuild_interval_ms: 86400000
  max_sessions_per_reduce: 1000
  max_reduce_span_seconds: 604800
  dirty_claim_ttl_seconds: 300
```

An ingest flush interval of `0` commits each request through the normal batch
path. A positive value coalesces requests in memory before commit; dirty writes
still follow successful lake flushes. Physical maintenance and summary rebuild
share the fenced PostgreSQL lease runner. Dirty-row reduction uses row claims
to distribute work among replicas. See [async jobs](async-jobs.md) and
[event-time layout](design-event-time-layout.md).
