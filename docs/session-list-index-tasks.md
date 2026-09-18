# Session list index — implementation tasks

**Design:** [`session-list-index.md`](./session-list-index.md) · [`async-jobs.md`](./async-jobs.md)  
**Rule:** Do not ship a private session-index timer before Stage A (shared leases).  
**Legend:** sequential within a stage unless marked **[P]** (safe to parallelize with sibling **[P]** tasks in that stage).

---

## Stage 0 — Unblock list cost (no index yet)

Can start immediately; independent of Stages A–5.

- [x] **0.1** Explorer: remove `sessionCountOverrides` / window-wide obs count scan from Sessions list
- [x] **0.2** **[P]** Explorer: confirm list still renders from `sessions/search` alone; update any tests/docs that assumed dual scan
- [x] **0.3** **[P]** (optional) Document temporary list latency until Stage 3 lands

**Stage 0 notes (2026-09-17):** Server list path is `sessions/search` only. Removed dead `childOf` return + App loadMore re-fold (was only fed by the scan). Client-side aggregate fallback still folds. Count/fold parity with detail waits on `session_index` / later stages.

---

## Stage 0b — Prefer `traces` / `logs` in SQL compilers

Independent of Stage 0; can run **in parallel with Stage A**.

- [ ] **0b.1** Emit `traces` / `logs` (not `union_*`) from query compilers for new SQL
- [ ] **0b.2** Keep rewrite shim for external SQL still using `union_*` (briefly)
- [ ] **0b.3** Plan / ticket for deleting the shim (do not block later stages)
- [ ] **0b.4** Remove all remainging `union_*` from all places

---

## Stage A — Shared async jobs + leases (prerequisite for everything below)

Must complete before Stage 1 reduce/rebuild jobs. **Blocks Stages 1–4.**

- [ ] **A.1** Decide registry schema for `thelake_job_lease` (shared control schema vs first-tenant) — open Q #7
- [ ] **A.2** DDL: `thelake_job_lease` (+ index on `lease_until`); Postgres + SQLite paths
- [ ] **A.3** `src/async_jobs/`: `Job` trait, runner wake loop, config (`instance_id`, `lease_ttl_seconds`, `heartbeat_seconds`)
- [ ] **A.4** `lease.rs`: acquire / heartbeat / release (single race-safe UPSERT); unit tests for steal + renew
- [ ] **A.5** Wrap existing maintenance as jobs (`maintenance.compact`, `maintenance.metadata`); remove unleased-only behavior from `compaction/scheduler.rs`
- [ ] **A.6** Wire runner from `main.rs` (single registration point; no second interval for future jobs)
- [ ] **A.7** Verify: two replicas → only one runs compact per `scope_key`; single-node SQLite still works
- [ ] **A.8** **[P]** Metrics: lease acquire/steal/heartbeat; job duration/error

---

## Stage B — Dropdown prune on shared runner

Depends on **A**. Can run **in parallel with Stage 1** (DDL + dirty ingest).

- [ ] **B.1** Register `dropdown.prune` on shared runner (same lease module)
- [ ] **B.2** Remove any private prune timer if present

---

## Stage 1 — Index schema + batched dirty from ingest

Depends on **A** for “where DDL lives” conventions; dirty UPSERT itself does **not** need the runner. Prefer after A.2 so catalog patterns match.

- [ ] **1.1** Config: `session_index.*` + gate: `session_index.enabled` ⇒ reject if `ingest.flush_interval_seconds == 0`
- [ ] **1.2** Per-tenant DDL: `session_index` + indexes (list cursor / filters)
- [ ] **1.3** Per-tenant DDL: `session_index_dirty` (`PRIMARY KEY (session_id)`)
- [ ] **1.4** Ensure tables on tenant bootstrap / scope ensure path (same pattern as other catalog tables)
- [ ] **1.5** Ingest: after successful lake flush batch, fold distinct `session_id → {min_ts,max_ts}` in memory → **one** multi-row dirty UPSERT (never per span)
- [ ] **1.6** Dirty UPSERT best-effort: log + metric on failure; **do not** fail ingest
- [ ] **1.7** Metrics: `dirty_upserts` ≈ flush count (not span count); dirty depth gauge
- [ ] **1.8** Tests: multi-session batch → one UPSERT; coalesce-required config validation

---

## Stage 2 — `session_index.reduce` job

Depends on **A** + **1**.

- [ ] **2.1** `SessionIndexReduceJob`: acquire lease `(session_index.reduce, tenant)`
- [ ] **2.2** Claim dirty batch (`LIMIT N` / `max_sessions_per_reduce`); snapshot `updated_at`
- [ ] **2.3** Compute `[from,to]` per §6.5; always attach `record_date` + timestamp bounds
- [ ] **2.4** Time-scoped `GROUP BY` from `traces` → absolute UPSERT `session_index`
- [ ] **2.5** Ack: `DELETE … WHERE session_id IN (…) AND updated_at <= snapshot`
- [ ] **2.6** Clamp / chunk when `to - from > max_reduce_span` (decide open Q #2: chunk vs defer rebuild)
- [ ] **2.7** Heartbeat during long reduces; release on completion
- [ ] **2.8** Tests: dirty → reduce → index matches aggregate; concurrent dirty mid-reduce not lost; no dual-reduce under two holders
- [ ] **2.9** **[P]** Metrics: reducer lag, sessions/reduce, lease steal

---

## Stage 3 — List API reads index

Depends on **2** (index must be populated). Stage **0** should already be done so Explorer does not double-scan.

- [ ] **3.1** `POST /v1/llm/sessions/search` reads `session_index` (cursor `(start_time, session_id)` desc)
- [ ] **3.2** Filters: time range, agent, has-errors (match current list contract)
- [ ] **3.3** Rollout fallback flag: legacy `traces` aggregate when index empty / `session_index.enabled=false`
- [ ] **3.4** Detail paths unchanged (still `traces` / lake)
- [ ] **3.5** API / Explorer contract tests: list p95 independent of span volume in window
- [ ] **3.6** **[P]** Update Explorer/docs for index-backed list behavior

---

## Stage 4 — Rebuild (heal + ops)

Depends on **A** + **1** (+ ideally **2** so UPSERT path is shared). Can start **in parallel with Stage 3** if reduce UPSERT helpers are extracted.

- [ ] **4.1** Shared reduce/rebuild aggregation helper (DRY with Stage 2)
- [ ] **4.2** `SessionIndexRebuildJob`: leased `session_index.rebuild`; windowed re-aggregate from `traces`
- [ ] **4.3** Periodic last-N-days rebuild (crash / dirty-miss heal) — cadence open Q #4
- [ ] **4.4** Ops: `POST /v1/llm/sessions/index/rebuild` `{from,to}` enqueues/triggers leased rebuild
- [ ] **4.5** Test: truncate `session_index` → rebuild restores list; Parquet untouched

---

## Stage 5 — Promote hot list columns (optional follow-on)

Depends on **2**/**3** working; parallelizable with polish only.

- [ ] **5.1** Identify hot list filter columns still in MAP bags
- [ ] **5.2** Promote typed columns so reducer prefers them over MAP extraction
- [ ] **5.3** Update reducer SQL + any promotion specs

---

## Verification gate (before calling the work ready)

Depends on Stages **0**, **A**, **1–4** (and **0b** if compilers changed).

- [ ] **V.1** Success criteria in `session-list-index.md` §15 checked with evidence
- [ ] **V.2** Success criteria in `async-jobs.md` §12 checked (multi-replica lease)
- [ ] **V.3** Dirty UPSERT rate ≈ lake flush rate, not span rate
- [ ] **V.4** No reducer/rebuild SQL without `record_date` + timestamp bounds
- [ ] **V.5** Workspace / thelake test gate green for touched crates

---

## Dependency sketch

```text
0 ──────────────┐
0b ──[P with A]─┤
A ──────────────┼──► 1 ──► 2 ──┬──► 3 ──► V
                │              │
                └──► B [P]     └──► 4 [P with 3 if helpers shared]
                                   │
                                   └──► 5 [optional]
```

| Parallel groups | Tasks |
|---|---|
| Anytime early | Stage 0 ‖ Stage 0b ‖ Stage A start |
| After A | Stage B ‖ Stage 1 |
| After 1+2 helpers | Stage 3 ‖ Stage 4 |
| After list works | Stage 5 |

---

## Suggested first commit-sized slices

1. Stage **A** only (leases + maintenance migrated) — shipable alone.  
2. Stage **1** (DDL + dirty) — ingest-safe, list still lake.  
3. Stage **2** + **3** (reduce + list cutover).  
4. Stage **4** (rebuild/ops).  
5. Stage **0** anytime before or with slice 3 (Explorer).
