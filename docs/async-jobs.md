# Async jobs and multi-instance coordination

**Status:** Design (not implemented)  
**Applies to:** DuckLake maintenance (TWCS / expire / orphan cleanup), dropdown TTL prune, session-index reduce/rebuild, and any future background work  
**DRY rule:** one lease API, one scheduler loop, one job trait — do not invent a second “maintenance-only” lock and a third “session-index” lock

---

## 1. Problem

thelake may run **multiple processes** (replicas). Several background tasks must not race:

| Work | Safe if two replicas run it? |
|---|---|
| TWCS merge / DuckLake file rewrite | **No** — concurrent merges on the same scope corrupt or waste work |
| Snapshot expire / orphan cleanup | **No** — overlapping CALLS / delete races |
| Dropdown catalog TTL prune | Mostly idempotent, still wasteful; serialize |
| Session-index reduce (`GROUP BY` → UPSERT) | Result UPSERT is idempotent, but duplicate scans waste IO; serialize per tenant |
| Session-index rebuild window | Same |

**Today (`main`):** `start_maintenance_scheduler` in `src/compaction/scheduler.rs` starts an interval loop on **every** process that has maintenance enabled. There is **no** cross-instance lease. That is a latent multi-replica bug for compaction and must not be copied for session-index.

**Ingest** stays on every replica (correct). **Async reduce/maintain** must be coordinated.

---

## 2. Principles

1. **Ingest path stays hot and local** — never wait on a job lease to ack OTLP.  
2. **Background work is jobs** — same runner for maintenance and session-index.  
3. **Catalog Postgres holds coordination** — same DSN as DuckLake metadata / scope registry (not Explorer Supabase, not Redis).  
4. **Lease before side effects** — acquire → heartbeat → run → release; expired leases are stealable.  
5. **Durable dirty hints for session-index** — if only the lease holder reduces, every ingest replica must publish dirty `(session_id, time bounds)` to Postgres (not only process RAM).  
6. **SQLite / single-node** — lease table still used (trivial single holder) so code paths stay one.

---

## 3. Where the reducer runs

```text
Replica A/B/C …          (all accept ingest)
        │
        ├─► write traces (DuckLake)
        └─► UPSERT session_index_dirty  (best-effort, no lease)

Async job runner            (every replica ticks; only lease winner works)
        │
        ├─ try_acquire(job=session_index.reduce, scope=tenant)
        │     lose → skip
        │     win  →
        │            read dirty rows for tenant
        │            time-scoped aggregate FROM traces
        │            UPSERT session_index
        │            delete/ack dirty rows
        │            heartbeat / release
        └─ same runner also runs maintenance.* jobs
```

**When:** on the shared scheduler wake (configurable interval, e.g. 2–5s for `session_index.reduce`, existing 60s/300s for metadata/compaction). Not inline in the ingest request after commit (beyond the cheap dirty UPSERT).

**Not:** a per-request in-process-only TouchSet as the sole dirty channel (that breaks multi-instance). Optional process-local coalescing before dirty UPSERT is fine.

---

## 4. Shared coordination schema

Place in the **scope registry / catalog control schema** (same Postgres as `DuckLakeScopeResolver` mappings — one place operators already back up), not inside each tenant DuckLake data path.

```sql
-- Global to the deployment (registry DB)
CREATE TABLE thelake_job_lease (
  job_name        TEXT        NOT NULL,  -- e.g. 'maintenance.compact'
  scope_key       TEXT        NOT NULL,  -- tenant_id or '_global'
  holder_id       TEXT        NOT NULL,  -- instance id (hostname+pid+uuid)
  lease_until     TIMESTAMPTZ NOT NULL,
  heartbeat_at    TIMESTAMPTZ NOT NULL,
  meta            JSONB,                 -- optional progress
  PRIMARY KEY (job_name, scope_key)
);

CREATE INDEX thelake_job_lease_until
  ON thelake_job_lease (lease_until);
```

**Acquire (single SQL, race-safe):**

```sql
INSERT INTO thelake_job_lease (job_name, scope_key, holder_id, lease_until, heartbeat_at)
VALUES ($1, $2, $3, now() + $ttl, now())
ON CONFLICT (job_name, scope_key) DO UPDATE SET
  holder_id = EXCLUDED.holder_id,
  lease_until = EXCLUDED.lease_until,
  heartbeat_at = EXCLUDED.heartbeat_at
WHERE thelake_job_lease.lease_until < now()
   OR thelake_job_lease.holder_id = EXCLUDED.holder_id
RETURNING holder_id;
```

Caller wins only if returned `holder_id` equals self. Heartbeat extends `lease_until` while running long merges.

Optional run history (debug / metrics, not required for v1 correctness):

```sql
CREATE TABLE thelake_job_run (
  id           BIGSERIAL PRIMARY KEY,
  job_name     TEXT NOT NULL,
  scope_key    TEXT NOT NULL,
  holder_id    TEXT NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL,
  finished_at  TIMESTAMPTZ,
  status       TEXT NOT NULL,  -- ok|error|skipped
  detail       TEXT
);
```

---

## 5. Job catalog (one trait)

```text
Job {
  name() -> &'static str
  scope_keys(ctx) -> Vec<ScopeKey>   // tenants from registry, or [Global]
  interval() -> Duration
  run(ctx, scope) -> Result<()>      // called only while holding lease
}
```

| `job_name` | `scope_key` | Interval (order of) | Today’s code |
|---|---|---|---|
| `maintenance.compact` | per `tenant_id` | `maintenance.interval_seconds` (300s) | `MaintenanceExecutor` TWCS/ladder |
| `maintenance.metadata` | per `tenant_id` | `metadata_interval_seconds` (60s) | expire snapshots + orphan cleanup |
| `dropdown.prune` | `_global` | with metadata pass | `DropdownCatalog::prune_older_than_days` |
| `session_index.reduce` | per `tenant_id` | `session_index.reducer_interval_ms` | **new** |
| `session_index.rebuild` | per `tenant_id` | scheduled / ops-triggered | **new** |

**Refactor target:** replace ad-hoc `start_maintenance_scheduler` loop with `async_jobs::spawn_runner(config, jobs[])` that:

1. Wakes on `min(job.intervals)`.  
2. For each due `(job, scope)`: `try_acquire` → `run` with heartbeat → `release` / let TTL expire.  
3. Never runs two holders for the same `(job_name, scope_key)`.

Maintenance logic stays in `compaction::executor` (domain). Session-index reduce stays in a `session_index` module (domain). **Only** scheduling + leasing are shared.

---

## 6. Durable dirty set (session-index)

Per **tenant metadata schema** (next to `session_index`):

```sql
CREATE TABLE session_index_dirty (
  session_id   TEXT        NOT NULL,
  min_ts       TIMESTAMPTZ NOT NULL,
  max_ts       TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (session_id)
);
```

**Ingest (every replica, after successful `traces` commit of a coalesced/OTLP batch):**

Fold session_id → min/max ts **in memory across the batch**, then:

```sql
INSERT INTO session_index_dirty AS d (session_id, min_ts, max_ts, updated_at)
VALUES (...), (...), ...   -- one row per distinct session_id in the batch
ON CONFLICT (session_id) DO UPDATE SET
  min_ts = LEAST(d.min_ts, EXCLUDED.min_ts),
  max_ts = GREATEST(d.max_ts, EXCLUDED.max_ts),
  updated_at = EXCLUDED.updated_at;
```

Never once-per-span. Prefer `ingest.flush_interval_seconds > 0` whenever session-index is on so flushes (and dirty UPSERTs) stay coarse. Best-effort: failure logs + metric; does not fail ingest.

**Reduce job (lease holder):**

1. `SELECT … FROM session_index_dirty ORDER BY updated_at LIMIT N` (or `FOR UPDATE SKIP LOCKED` batch if ever multi-worker within one job — v1 single holder is enough).  
2. Compute `[from,to]` per § time-scope rules in [`session-list-index.md`](./session-list-index.md).  
3. Aggregate `traces` → UPSERT `session_index`.  
4. `DELETE FROM session_index_dirty WHERE session_id IN (…) AND updated_at <= batch_snapshot_ts` (avoid deleting newer touches).

In-process TouchSet becomes an **optional coalesce buffer** before dirty UPSERT, not the source of truth for “what to reduce.”

---

## 7. Interaction with ingest and query

| Path | Lease? | Notes |
|---|---|---|
| OTLP / write `traces` | No | All replicas |
| Dirty UPSERT | No | All replicas |
| `sessions/search` (index) | No | Read Postgres index |
| Session detail (`traces`) | No | Read DuckLake |
| Compact / expire / reduce / rebuild | **Yes** | Per job + scope |

List lag = dirty publish latency + reducer interval + lease wait (usually seconds).

---

## 8. Failure modes

| Failure | Behavior |
|---|---|
| Holder crashes mid-job | `lease_until` expires; another replica steals |
| Holder alive but slow | Heartbeat extends lease; configure TTL > typical pass |
| Dirty UPSERT fails | Spans durable; periodic `session_index.rebuild` heals |
| Two replicas try acquire | One wins; other skips — no dual compact |
| SQLite single process | Same tables; always wins acquire |

---

## 9. Staged delivery (DRY order)

Full checkbox list: [`session-list-index-tasks.md`](./session-list-index-tasks.md).

| Stage | Work |
|---|---|
| **A** | Introduce `thelake_job_lease` + `async_jobs` runner; migrate **existing** maintenance scheduler onto it (behavior unchanged, now single-winner) |
| **B** | Move dropdown prune onto same runner as `dropdown.prune` |
| **C** | `session_index` DDL + durable dirty + `session_index.reduce` job |
| **D** | List API reads index; rebuild job / ops endpoint |

Do **not** ship session-index reduce on a second homemade timer. Do **not** leave maintenance unleased after session-index lands.

---

## 10. Code layout (suggested)

```text
src/async_jobs/
  mod.rs          // runner, wake loop
  lease.rs        // acquire / heartbeat / release (Postgres + SQLite)
  job.rs          // Job trait
src/compaction/   // MaintenanceCompactJob, MaintenanceMetadataJob (wrappers)
src/session_index/
  dirty.rs
  reduce.rs       // SessionIndexReduceJob
  rebuild.rs
```

`main.rs` registers jobs once; no second `tokio::spawn` interval for session-index.

---

## 11. Config sketch

```yaml
async_jobs:
  instance_id: null   # default: hostname-pid-uuid
  lease_ttl_seconds: 120
  heartbeat_seconds: 30

maintenance:          # existing knobs; runner reads intervals
  enabled: true
  interval_seconds: 300
  metadata_enabled: true
  metadata_interval_seconds: 60

session_index:
  enabled: true
  reducer_interval_ms: 3000
  max_sessions_per_reduce: 500
  max_reduce_span: 7d
```

---

## 12. Success criteria

1. Two thelake replicas never run `maintenance.compact` on the same `scope_key` concurrently.  
2. Session-index reduce uses the **same** lease module as maintenance.  
3. Ingest on replica A dirties a session; replica B (lease holder) reduces it.  
4. No ingest request blocks on lease acquisition.  
5. Single-node SQLite path uses the same Job + lease code.

---

## 13. References

- Current unleased loop: `src/compaction/scheduler.rs`  
- Pass body: `src/compaction/executor.rs`  
- Session list index: [`session-list-index.md`](./session-list-index.md)  
- Design maintenance section: [`design.md`](./design.md)
