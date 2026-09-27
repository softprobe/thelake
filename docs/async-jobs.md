# Async jobs and multi-instance coordination

## Current coordination model

Every replica ingests. Ingest does not acquire a job lease. After trace commit, it best-effort upserts durable dirty session hints in PostgreSQL.

| Work | Coordination | Why |
|---|---|---|
| Physical-scope maintenance (TWCS, expire, orphan cleanup) | Registry PostgreSQL lease with epoch fencing | Concurrent physical maintenance is unsafe |
| Workspace session-summary rebuild | Registry PostgreSQL lease with epoch fencing | Periodic heavy scan should run once per workspace |
| Session-summary reduce | Dirty-row claims with `FOR UPDATE SKIP LOCKED` and a claim TTL | UPSERT is repeatable; independent replicas can drain disjoint batches |
| Ingest and dirty UPSERT | No lease | Ingest must not wait for job coordination |

Do not add a queue framework, advisory locks, Redis, or a second maintenance lock. The shared `spawn_runner` is for leased work only. The reducer loop is separate and uses claim ownership in `session_summary_dirty`.

## Registry lease

`{registry}.thelake_job_lease` is the coordination table in the catalog registry schema. Its primary key is `(job_name, scope_key)`; the row stores `holder_id`, monotonically increasing `epoch`, `lease_until`, and `heartbeat_at`.

Acquisition uses a race-safe PostgreSQL UPSERT. First acquisition starts at epoch 1; expired acquisition advances the epoch. Heartbeat and release require both holder and epoch. Release expires the row instead of deleting it so the next acquisition can advance the durable fence.

The runner passes the acquired token to the job and cancels the job future after heartbeat loss. Maintenance checks lease loss between major actions. This is process-level fencing: it does not atomically fence a DuckLake/object-store operation already in progress, so a check/use window remains in v1.

Leased jobs are:

- `physical_scope_maintenance`, keyed by the maintenance scope key.
- `workspace_session_summary_rebuild`, keyed by workspace.

`workspace_session_summary_reduce` does not acquire a registry lease.

## Dirty-row claims

Both isolated and shared-scope `session_summary_dirty` tables have nullable `claim_holder` and `claim_until` columns. Claiming runs in a short transaction: select eligible rows ordered by `updated_at`, lock with `FOR UPDATE SKIP LOCKED`, then update their claim token and expiry. Shared tables constrain each batch by `tenant_id`.

Each claim attempt has a unique token. The dirty table keeps a per-row `generation`; a PostgreSQL trigger increments it on every update, including writes from older application versions. The claim returns that generation. The reducer aggregates from DuckLake, then opens a short publication transaction that locks and verifies all claimed rows still have the same owner and generation before UPSERTing absolute summary values. It deletes only rows whose generation still matches the claim; changed rows keep their dirty hint and have the claim cleared for retry. Timestamp ties, wall-clock corrections, and older app-clock writers cannot make a newer hint look old.

Ingest UPSERTs merge `min_ts` and `max_ts`, set `updated_at` with PostgreSQL `clock_timestamp()`, and leave claim columns untouched. Claim and publication transactions are short; ingest never waits on a lease or an entire reduce pass. A dirty UPSERT can briefly contend with either transaction on the same PostgreSQL row.

A crashed reducer's rows become claimable after `dirty_claim_ttl_seconds`. The reducer loop drains batches per workspace and waits `reducer_interval_ms` between passes. Start with one reducer loop per process.

## Failure behavior

| Event | Behavior |
|---|---|
| Two replicas acquire the same active lease | One wins; the other skips |
| A lease expires and is stolen | Epoch advances; stale heartbeat/release fails; the runner cancels the old job future |
| A reducer crashes | Claim expires and another replica reclaims it |
| Ingest updates a claimed row | Dirty bounds merge; generation ack preserves the newer update |
| An old claim token acknowledges after reclaim | Delete and claim-clear affect zero rows |
| Dirty UPSERT fails | Ingest remains successful; the periodic summary rebuild can repair the projection |

PostgreSQL lease durations use whole seconds (`max(1)`); configure `lease_ttl_seconds` well above `heartbeat_seconds` and typical pass duration. The dirty claim TTL should exceed normal p99 reduce batch duration.

## Configuration

```yaml
async_jobs:
  instance_id: null
  lease_ttl_seconds: 120
  heartbeat_seconds: 30

session_summary:
  reducer_interval_ms: 10000
  dirty_claim_ttl_seconds: 300
  rebuild_interval_ms: 86400000
  max_sessions_per_reduce: 1000
  max_reduce_span_seconds: 604800
```

## Code locations

- Lease runner and store: `src/async_jobs/`
- Maintenance/rebuild registration: `src/compaction/scheduler.rs`
- Maintenance actions: `src/compaction/engine.rs`
- Dirty schema and claim/ack: `src/session_summary/ddl.rs`, `src/session_summary/reduce.rs`
- Reducer loop and rebuild job: `src/session_summary/job.rs`
- Session list design: [`session-list-summary.md`](./session-list-summary.md)
