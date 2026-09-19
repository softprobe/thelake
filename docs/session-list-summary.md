# Session list summary

**Status:** Stage 1 implemented (DDL + dirty UPSERT; reduce not yet)  
**Baseline:** `thelake` / `sp-llm` `main`  
**Supersedes:** ChatGPT “Design Session Summaries” share; earlier drafts that put the directory in Explorer Supabase, dual-wrote DuckLake `session_facts`, or used a long-lived in-memory span counter

---

## 1. Summary

Session **list** must not scan DuckLake for every UI load. Session **detail** must keep reading DuckLake.

**Single derived store:** skinny `session_summary` in **thelake catalog Postgres** (per tenant metadata schema).  
**Not written:** any DuckLake session-summary table.  
**Not written by:** Explorer (read-only for this path).

```text
ingest (every replica, coalesced batches) → DuckLake `traces`
    then ONE batched dirty UPSERT (distinct session_ids in batch)

async job runner       → lease session_summary.reduce per tenant
                       → time-scoped aggregate FROM traces
                       → UPSERT session_summary; ack dirty rows

list   ← session_summary
detail ← traces
```

**Hard rule:** reducer/rebuild SQL always includes `record_date` + timestamp `[from,to]` so Parquet files are pruned.

**Dirty write rule:** never per span — once per successful lake flush batch. `session_summary.enabled` requires `ingest.flush_interval_seconds > 0` (soft coalesce).

**Where/when the reducer runs:** not inline on ingest. It is an **async job** on the shared job runner (same framework as DuckLake maintenance), under a **Postgres job lease** so only one replica reduces a tenant at a time. See [`async-jobs.md`](./async-jobs.md).

---

## 2. Problem

### 2.1 Product need

Explorer Sessions list: triage rows (`session_id`, agent, times, steps, errors, tokens/cost). Filters: range, agent, has-errors. Cursor pagination.

Detail: one session’s observations / payloads from thelake.

### 2.2 Main today

| Path | Behavior | Cost |
|---|---|---|
| List | `sessions/search` → `GROUP BY session_id` over all spans in window | Grows with window volume |
| List (Stage 0) | Explorer **no longer** runs `sessionCountOverrides` / window `observations/search` | Half the previous Explorer load; list still lake-bound until Stage 3 |
| Detail | session-scoped reads | Correct |

### 2.3 Constraints

1. Explorer does not ingest → thelake owns summary refresh.  
2. No dual derived table in DuckLake + Postgres.  
3. Evidence only in DuckLake; Postgres holds summary + dirty queue + job leases.  
4. Catalog Postgres already exists per tenant `metadata_schema`.  
5. Multiple thelake replicas → async work needs **one** Postgres lease system (shared with maintenance).

---

## 3. Goals and non-goals

### Goals

- List cost ~ page size / filter selectivity, not spans-in-window.  
- Detail unchanged (`traces` by `session_id`).  
- Summary **reconstructible from `traces`**; summary loss ≠ evidence loss.  
- Summary numbers come from **re-aggregating DuckLake**, not a second long-lived counter.  
- Reducer is an **async leased job** (DRY with maintenance) — see [`async-jobs.md`](./async-jobs.md).  
- Simple ops: no Kafka / Redis / CH / Elastic / separate summary service.  
- Keep forever-cheap evidence retention.

### Non-goals

- List error counts == detail primary-error tree.  
- Subsecond multi-tenant dashboards.  
- Lake-native forever session analytics rollups.  
- Full-text over prompts on list.  
- OPEN/FINALIZED session state machine in v1.

---

## 4. Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│ Clients                                                      │
│   list  → POST /v1/llm/sessions/search  (session_summary)      │
│   detail→ GET  /v1/llm/sessions/{id}…   (traces)             │
└────────────────────────────▲─────────────────────────────────┘
                             │
┌────────────────────────────┴─────────────────────────────────┐
│ thelake replicas (N)                                         │
│   ingest (all): traces commit → UPSERT session_summary_dirty   │
│   async_jobs runner (all tick; Postgres lease = single winner│
│     per job_name + scope_key):                               │
│       maintenance                                            │
│       session_summary.reduce / session_summary.rebuild           │
│   query: list → session_summary; detail → traces               │
└───────────────┬────────────────────────────▲─────────────────┘
                ▼                            │
┌───────────────────────────┐   ┌────────────┴─────────────────┐
│ DuckLake                  │   │ Catalog Postgres             │
│   traces, logs, scores    │   │   thelake_job_lease (shared) │
│   evidence only           │   │   session_summary + _dirty     │
│                           │   │   (per tenant meta schema)   │
└───────────────────────────┘   └──────────────────────────────┘
```

| Data | Store | Authority |
|---|---|---|
| Span bodies / attrs / events | DuckLake `traces` | **SoT** |
| List triage fields | `session_summary` | Derived, rebuildable |
| Dirty session hints | `session_summary_dirty` | Ephemeral work queue |
| Who runs async work | `thelake_job_lease` | Coordination only |

```text
session_summary MUST be reconstructible from traces.
traces MUST NOT depend on session_summary.
```

**Multi-instance / DRY:** do not give session-summary its own timer or lock. It is a job on the shared runner. Today’s maintenance scheduler has **no** cross-replica lease — that gap is fixed by the same design. Details: [`async-jobs.md`](./async-jobs.md).

---

## 5. Data model

### 5.1 `session_summary` (catalog Postgres / SQLite metadata)

Per tenant metadata schema (not a DuckLake Parquet table):

```sql
CREATE TABLE session_summary (
  session_id          TEXT        NOT NULL,
  start_time          TIMESTAMPTZ NOT NULL,
  end_time            TIMESTAMPTZ,
  observation_count   BIGINT      NOT NULL DEFAULT 0,
  error_count         BIGINT      NOT NULL DEFAULT 0,
  input_tokens        BIGINT,
  output_tokens       BIGINT,
  total_tokens        BIGINT,
  total_cost          DOUBLE PRECISION,
  agent_name          TEXT,
  user_id             TEXT,          -- optional v1.1
  model_name          TEXT,          -- optional v1.1
  updated_at          TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (session_id)
);

CREATE INDEX session_summary_recent
  ON session_summary (start_time DESC, session_id);
CREATE INDEX session_summary_agent
  ON session_summary (agent_name, start_time DESC, session_id)
  WHERE agent_name IS NOT NULL;
CREATE INDEX session_summary_errors
  ON session_summary (start_time DESC, session_id)
  WHERE error_count > 0;
```

### 5.2 `session_summary_dirty` (same tenant schema)

Durable touch queue so **any** ingest replica can mark work for the lease-holding reducer:

```sql
CREATE TABLE session_summary_dirty (
  session_id   TEXT        NOT NULL,
  min_ts       TIMESTAMPTZ NOT NULL,
  max_ts       TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (session_id)
);
```

**Write amplification rule:** never UPSERT dirty **per span**. Dirty writes are tied to the **DuckLake commit batch** only:

1. While building/flushing an ingest batch, accumulate in memory  
   `Map<session_id, {min_ts, max_ts}>` over spans in that batch.  
2. After the `traces` commit succeeds, issue **one** batched dirty UPSERT  
   (multi-row `INSERT … ON CONFLICT` for the distinct session_ids in the batch).  
3. If the lake commit fails, do not write dirty.

That is already “batched” whenever the OTLP path commits many spans together. To keep commit (and thus dirty) frequency low in multi-replica production, **session_summary requires soft coalesce**:

```yaml
ingest:
  flush_interval_seconds: > 0   # required when session_summary.enabled
session_summary:
  enabled: true
```

Default thelake today is flush-through (`flush_interval_seconds: 0`) — fine for tiny demos, but with session_summary it would dirty-UPSERT once per OTLP request. Soft coalesce merges requests into fewer DuckLake writes → fewer dirty UPSERTs. Upstream collector batching still matters; coalesce amortizes further.

Optional: coalesce dirty rows in-process for a few hundred ms before Postgres UPSERT **only if** still within the same post-commit hook; do not add a second timer that races the job runner. Prefer one dirty write per successful lake flush.
### 5.3 Semantics

| Field | Summary | Detail |
|---|---|---|
| `observation_count` | `COUNT(DISTINCT span_id)` from `traces` | Deduped observations |
| `error_count` | `#` with `status_code = 'ERROR'` (coarse) | Primary-error / timeline |
| tokens / cost / agent | From same lake aggregate | From spans |
| payloads | Never | attrs / events |

After a successful reduce for session S, summary(S) matches the lake aggregate for S over the chosen `[from,to]`.

### 5.4 Not stored

attrs/events/prompts, JSONB labels GIN (defer), version/hash/OPEN state, any DuckLake summary mirror.

---

## 6. Reducer: async job + lake micro-batch

### 6.1 Where and when

| | |
|---|---|
| **Where** | `SessionSummaryReduceJob` on the shared `async_jobs` runner (same process binary as maintenance) |
| **When** | On `session_summary.reducer_interval_ms` (e.g. 2–5s), after winning `thelake_job_lease` for `(session_summary.reduce, tenant_id)` |
| **Not** | Inside the ingest HTTP/gRPC handler beyond the cheap dirty UPSERT |
| **Coordination** | [`async-jobs.md`](./async-jobs.md) — same lease table as `maintenance.compact` |

`session_summary.rebuild` is the same runner, longer interval or ops-triggered, also leased.

### 6.2 Why not RAM-only TouchSet

With multiple thelake instances, replica A’s in-memory dirty set is invisible to replica B. If only the lease holder reduces, dirty state **must** live in Postgres (`session_summary_dirty`). Optional process-local coalesce before dirty UPSERT is an optimization only.

Pure in-memory absolute/delta counters are also rejected as SoT (crash / double-add). Arithmetic always comes from `traces`.

### 6.3 Flow

```text
ingest batch (soft coalesce flush or single OTLP commit)
  ├─1─► commit all spans in batch to DuckLake `traces`
  └─2─► ONE batched UPSERT session_summary_dirty
          for distinct session_ids in that batch
          (min_ts/max_ts folded in memory first — never per span)

SessionSummaryReduceJob (lease winner only)
  ├─1─► SELECT dirty rows LIMIT N (snapshot updated_at)
  ├─2─► for each id: [from,to] per §6.5
  ├─3─► time-scoped GROUP BY FROM traces
  ├─4─► UPSERT session_summary (absolute replace of aggregates)
  └─5─► DELETE dirty rows with updated_at <= snapshot
```

Config gate: `session_summary.enabled` implies `ingest.flush_interval_seconds > 0` (reject or auto-enable coalesce at startup — pick one in implementation; prefer **reject** so ops is explicit).
### 6.4 Reducer SQL (time scope required)

```sql
SELECT session_id,
       MIN(timestamp) AS start_time,
       MAX(COALESCE(end_timestamp, timestamp)) AS end_time,
       COUNT(DISTINCT span_id) AS observation_count,
       SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
       SUM(total_tokens) AS total_tokens,
       ...
FROM traces
WHERE session_id IN (...)
  AND session_id <> ''
  AND <exclude recording>
  AND record_date BETWEEN DATE '...' AND DATE '...'   -- REQUIRED partition prune
  AND CAST(timestamp AS TIMESTAMP_NS) >= ...            -- REQUIRED
  AND CAST(timestamp AS TIMESTAMP_NS) <= ...
GROUP BY session_id;
```

```sql
INSERT INTO session_summary AS s (...)
VALUES (...)
ON CONFLICT (session_id) DO UPDATE SET
  start_time        = EXCLUDED.start_time,
  end_time          = EXCLUDED.end_time,
  observation_count = EXCLUDED.observation_count,
  error_count       = EXCLUDED.error_count,
  total_tokens      = EXCLUDED.total_tokens,
  agent_name        = EXCLUDED.agent_name,
  updated_at        = EXCLUDED.updated_at;
```

**50 then +5 → 55** when `[from,to]` covers the session so far. Lease prevents two replicas from double-scanning the same tenant; UPSERT remains idempotent if a steal retries.

### 6.5 Mandatory `[from, to]`

```text
to   = max(dirty.max_ts, now())
from = least(coalesce(session_summary.start_time, dirty.min_ts), dirty.min_ts)
```

Plus `record_date BETWEEN date(from) AND date(to)`. Clamps: `max_reduce_span`, `max_sessions_per_reduce`; oversized windows chunk or defer to `session_summary.rebuild`.

### 6.6 Late spans

No FINALIZED. Late span → dirty UPSERT → next leased reduce replaces the summary row from `traces`.

---

## 7. Read path

### 7.1 List

`POST /v1/llm/sessions/search` → select from `session_summary` (cursor on `(start_time, session_id)` desc). Steady-state path does **not** scan `traces`.

Fallback flag: legacy aggregate over `traces` for empty summary / rollout only.

### 7.2 Detail

Unchanged: `GET …/sessions/{id}`, observations, recording — read **`traces`**.

### 7.3 Explorer

- Keep calling `sessions/search` via Worker.  
- **Done (Stage 0):** removed `sessionCountOverrides` — list uses server `SessionSummary` counts only; no window `observations/search`.  
  Until Stage 3 (`session_summary`), list latency still tracks lake `sessions/search` cost; STEPS/RESULT may diverge from detail (bubbled errors / duplicate spans). Child-session folding on the server list path is gone with the scan (client-side aggregate fallback still folds); restore via summary fields or server `roots_only` later.  
- Findings/agents stay in Supabase UI join by `session_id`.  
- Explorer never writes `session_summary`.

### 7.4 Pagination

Cursor only; no per-page `COUNT(*)`; non-time sorts may stay `cursor_supported=false`.

---

## 8. Rebuild

Same aggregate as reducer; **`[from, to]` required** (ops must pass a window — never whole-lake):

```text
rebuild([from, to]):
  SELECT ... FROM traces
  WHERE record_date BETWEEN date(from) AND date(to)
    AND timestamp >= from AND timestamp <= to
    AND session_id present AND not recording
  GROUP BY session_id
  → absolute UPSERT session_summary
```

Triggers: ops/CLI with explicit window; periodic last-N-days (crash heal); optional post-promotion backfill.

---

## 9. Consistency

| Event | `traces` | Summary |
|---|---|---|
| Commit OK, reduce pending | OK for detail | List lags seconds |
| Reduce fails | Durable | Stale until retry/rebuild |
| Process crash / lease expiry | Durable | Another replica steals lease; dirty rows remain until reduce |
| Truncate `session_summary` | Untouched | Rebuild restores list |

Forbidden: fail ingest on summary errors; store payloads in Postgres; DuckLake summary table; RAM counter as SoT for list numbers.

---

## 10. API / config sketch

Keep `SessionSearchRequest` / `SessionSummary` shapes.

```yaml
# Shared runner: see async-jobs.md
ingest:
  flush_interval_seconds: 2   # required when session_summary.enabled (> 0)
session_summary:
  enabled: true
  reducer_interval_ms: 3000
  max_sessions_per_reduce: 500
  max_reduce_span: 7d
```

Startup validation: if `session_summary.enabled` and `ingest.flush_interval_seconds == 0`, fail config load with a clear message (force soft coalesce). Dirty UPSERT count should track **lake flush count**, not span count.

Ops: `POST /v1/llm/sessions/summary/rebuild` `{from,to}` triggers leased `session_summary.rebuild`; metrics for dirty depth, reducer lag, lease steal, dirty_upserts vs span_writes.

---

## 11. Evidence layout / naming

- `traces` partitioned by `record_date`, sorted with `session_id`.  
- New SQL uses **`traces` / `logs`**, not `union_*`.  
- Stage **0b**: remove `union_*` emitters from query compilers; keep rewrite shim briefly if external SQL still uses old names, then delete shim.  
- Promote list filter columns so reducer prefers typed columns over MAP bags.

---

## 12. Staged delivery

Checkbox task list (sequential order + **[P]** parallel marks): [`session-list-summary-tasks.md`](./session-list-summary-tasks.md).

| Stage | Work |
|---|---|
| **0** | Explorer: drop list count-scan |
| **0b** | Emit `traces`/`logs` in compilers; plan delete of `union_*` |
| **A** | Shared `async_jobs` + `thelake_job_lease`; migrate maintenance scheduler onto it ([`async-jobs.md`](./async-jobs.md) stage A) |
| **1** | `session_summary` + `session_summary_dirty` DDL; ingest dirty UPSERT |
| **2** | `session_summary.reduce` job on shared runner |
| **3** | `sessions/search` reads summary |
| **4** | Leased `session_summary.rebuild` (periodic + ops) |
| **5** | Promote hot list columns |

**Do not** ship a private session-summary timer before stage A. Maintenance must gain leases in the same change set family.

---

## 13. Rejected

| Idea | Why |
|---|---|
| DuckLake `session_facts` + Postgres copy | Dual derived truth |
| Explorer Supabase writer | No ingest there |
| Long-lived RAM absolute/delta counter | Crash / retry hazards; multi-instance blind |
| Per-replica in-memory TouchSet as sole dirty channel | Other replicas never see dirty sessions |
| Private session-summary `tokio::interval` + ad-hoc lock | Violates DRY; maintenance already needs shared leases |
| Dual maintenance lock + session-summary lock | Two coordination systems |
| Per-span Postgres UPDATE | Amplification |
| Redis/Elastic/CH | Wrong economics |

---

## 14. vs ChatGPT share

Kept: disposable summary, cursor paging, batched updates, evidence ≠ summary.  
Dropped: DuckLake summary table, dual publish, OPEN/FINALIZED, RAM SessionReducer as arithmetic SoT.  
Replaced reducer with: **durable dirty + leased async job + `FROM traces` aggregate** ([`async-jobs.md`](./async-jobs.md)).

---

## 15. Success criteria

1. List p95 ≠ f(spans-in-window).  
2. Detail still lake-only for payloads.  
3. Truncating summary leaves Parquet intact; rebuild restores list.  
4. Ingest ≠ blocked on summary.  
5. No DuckLake session-summary table.  
6. No Explorer window-wide obs scan on list.  
7. After reduce(S), summary(S) matches aggregate(S) on `traces` over the chosen `[from,to]`.  
8. No reducer/rebuild SQL ships without `record_date` + timestamp bounds.  
9. Session-summary reduce uses the same lease module as maintenance; two replicas never dual-compact or dual-reduce one tenant.  
10. Dirty UPSERT rate ≈ lake flush rate (coalesced batches), never ≈ span rate.

---

## 16. Open questions

1. Exact timestamp literal / `TIMESTAMP_NS` helpers shared with existing query SQL.  
2. Behavior when `to - from > max_reduce_span` (chunk vs defer to rebuild).  
3. `user_id` / `model_name` in v1 vs later.  
4. Rebuild cadence.  
5. DDL bootstrap vs existing `promotion_specs` ensure path.  
6. Timeline to delete `union_*` rewrite shim entirely.  
7. Registry schema name for `thelake_job_lease` (shared vs first tenant) — decide with scope-resolver layout.

---

## 17. References

- Async jobs / leases: [`async-jobs.md`](./async-jobs.md)  
- [`positioning.md`](./positioning.md)  
- [`design.md`](./design.md)  
- [`adhoc-duckdb-ducklake.md`](./adhoc-duckdb-ducklake.md)  
- Current unleased maintenance: `src/compaction/scheduler.rs`  
- `sp-llm/apps/explorer/design/data-facts.md`  
- `src/api/llm/query.rs`  
- https://chatgpt.com/share/6aacaf0f-3a90-83e8-9371-55563225cebb
