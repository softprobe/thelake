# Session list summary — implementation tasks

**Design:** [`session-list-summary.md`](./session-list-summary.md) · [`async-jobs.md`](./async-jobs.md)  
**Rule:** Do not ship a private session-summary timer before Stage A (shared leases).  
**Legend:** sequential within a stage unless marked **[P]** (safe to parallelize with sibling **[P]** tasks in that stage).

---

## Stage 0 — Unblock list cost (no summary yet)

Can start immediately; independent of Stages A–5.

- [x] **0.1** Explorer: remove `sessionCountOverrides` / window-wide obs count scan from Sessions list
- [x] **0.2** **[P]** Explorer: confirm list still renders from `sessions/search` alone; update any tests/docs that assumed dual scan
- [x] **0.3** **[P]** (optional) Document temporary list latency until Stage 3 lands

**Stage 0 notes (2026-09-17):** Server list path is `sessions/search` only. Removed dead `childOf` return + App loadMore re-fold (was only fed by the scan). Client-side aggregate fallback still folds. Count/fold parity with detail waits on `session_summary` / later stages.

---

## Stage 0b — Prefer `traces` / `logs` in SQL compilers

Independent of Stage 0; can run **in parallel with Stage A**.

- [x] **0b.1** Emit `traces` / `logs` (not `union_*`) from query compilers for new SQL
- [x] **0b.2** Keep rewrite shim for external SQL still using `union_*` (briefly)
- [x] **0b.3** Plan / ticket for deleting the shim (do not block later stages)
- [x] **0b.4** Remove all remaining `union_*` from first-party emitters/fixtures (shim kept)

**Stage 0b notes:** Compilers/fixtures emit `traces`/`logs`/`metrics`. Legacy `union_*` remains rewrite-only in `rewrite_reserved_telemetry_view_names` (code PR `feat/prefer-traces-logs`). Internal `union_metrics_layout_*` helpers are not the SQL shim.

---

## Stage A — Shared async jobs + leases (prerequisite for everything below)

Must complete before Stage 1 reduce/rebuild jobs. **Blocks Stages 1–4.**

- [x] **A.1** Decide registry schema for `thelake_job_lease` → same schema as `scope_registry` (`DuckLakeScopeResolver.registry_schema`)
- [x] **A.2** DDL: `thelake_job_lease` (+ index on `lease_until`); Postgres in `ensure_registry`; sqlite/single-node via `MemoryLeaseStore`
- [x] **A.3** `src/async_jobs/`: `Job` trait, runner interval loop, config (`instance_id`, `lease_ttl_seconds`, `heartbeat_seconds`); reject `heartbeat >= lease_ttl`
- [x] **A.4** `lease.rs`: acquire / heartbeat / release (race-safe UPSERT); Memory unit tests + Postgres (`make test-lease-pg`) for steal + renew + concurrent race
- [x] **A.5** Wrap existing maintenance as one job_name `maintenance` per tenant: each pass runs metadata + TWCS when enabled (TWCS no-ops when nothing to merge); always release after run; prune once per interval; remove unleased interval from `compaction/scheduler.rs`
- [x] **A.6** Wire runner from `start_maintenance_scheduler` / `main.rs` (single registration; no second interval)
- [x] **A.7** Verify: lease lose skips `run`; MemoryLeaseStore for non-Postgres; concurrent acquire → one winner; release after Ok/Err/panic
- [x] **A.8** **[P]** Metrics: lease acquire win/lose/error; Memory steal; heartbeat failure; job error

**Stage A notes:** No sticky leases / CompactWakeGate — lake decides TWCS work; lease only serializes the pass. Open/attach failure is `Err`. No `thelake_job_run` history table.

---

## Stage B — Remove Dropdown catalog

- [x] **B.1** Remove dropdown catalog (`src/catalog/`, config, writer upsert, API routes, maintenance prune)
- [x] **B.2** Clean docs/config/scripts — no `dropdown_catalog` / `ui_dropdown_catalog` / `/v1/catalog/*` mentions

---

## Stage 1 — Summary schema + batched dirty from ingest

Depends on **A** for “where DDL lives” conventions; dirty UPSERT itself does **not** need the runner. Prefer after A.2 so catalog patterns match.

- [x] **1.1** Config: `session_summary.*` knobs; postgres catalog ⇒ always on; coalesce handles flush 0 and >0 alike
- [x] **1.2** Per-tenant DDL: `session_summary` + indexes (list cursor / filters)
- [x] **1.3** Per-tenant DDL: `session_summary_dirty` (`PRIMARY KEY (session_id)`)
- [x] **1.4** Ensure tables on tenant bootstrap / scope ensure path (same pattern as other catalog tables)
- [x] **1.5** Ingest: after successful lake flush batch, fold distinct `session_id → {min_ts,max_ts}` in memory → **one** multi-row dirty UPSERT (never per span)
- [x] **1.6** Dirty UPSERT best-effort: log + metric on failure; **do not** fail ingest
- [x] **1.7** Metrics: `dirty_upserts` ≈ flush count (not span count); `dirty_upsert_errors` (no depth gauge in Stage 1 — depth is Stage 2)
- [x] **1.8** Tests: full matrix — config gates; fold empty/single/multi/missing session_id; LEAST/GREATEST merge; ensure idempotent; coalesce flush → dirty rows; lake Err → no dirty; dirty Err → ingest still Ok; disabled → no dirty
- [x] **1.9** DRY: single post-traces-commit hook on coalesce spans writer (one mode for flush 0 and >0); no duplicated fold/UPSERT

---

## Stage 2 — `session_summary.reduce` job

Depends on **A** + **1**.

- [x] **2.1** `SessionSummaryReduceJob`: acquire lease `(session_summary.reduce, tenant)`
- [x] **2.2** Claim dirty batch (`LIMIT N` / `max_sessions_per_reduce`); snapshot `updated_at`
- [x] **2.3** Compute `[from,to]` per §6.5; always attach `record_date` + timestamp bounds
- [x] **2.4** Time-scoped `GROUP BY` from `traces` → absolute UPSERT `session_summary`
- [x] **2.5** Ack: `DELETE … WHERE session_id IN (…) AND updated_at <= snapshot`
- [x] **2.6** Clamp when `to - from > max_reduce_span` (Q #2: **clamp**, no chunk; Stage 4 rebuild for early history)
- [x] **2.7** Heartbeat during long reduces; release on completion (shared `spawn_runner`)
- [x] **2.8** Tests: field-accuracy DuckDB matrix + postgres claim/ack/upsert; concurrent dirty mid-reduce not lost
- [x] **2.9** **[P]** Metrics: reducer lag, sessions/reduce; dirty depth gauge (Postgres `count(*)` on claim)

**Stage 2 notes (2026-09-18):** Implemented on `feat/session-summary-reduce`. Promoted-only reduce SQL (zero `attributes` MAP). Canonical `traces-query-hot-attrs.yaml` ensured on postgres scope ensure. Clamp (not chunk) for oversized windows.

| Task | Evidence |
|---|---|
| 2.1 / 2.7 | `SessionSummaryReduceJob` on same `spawn_runner` as maintenance (`compaction/scheduler.rs`) |
| 2.2–2.5 | `session_summary/reduce.rs` claim → lake aggregate → UPSERT → ack |
| 2.3–2.4 | `reduce_sql.rs` — `record_date` + ts + `session_id IN`; typed SUMs; unit asserts no `attributes` |
| 2.6 | `compute_reduce_bounds` clamp; accuracy test `clamp_window_excludes_early_history` |
| 2.8 | `reduce_accuracy_tests.rs` (all fields/cases) + postgres claim/ack/upsert tests |
| 2.9 | `session_summary_dirty_depth`, `sessions_reduced`, `reducer_lag_seconds` instruments |

---

## Stage 3 — List API reads summary

Depends on **2** (summary must be populated). Stage **0** should already be done so Explorer does not double-scan.

- [x] **3.1** `POST /v1/llm/sessions/search` reads `session_summary` (cursor `(start_time, session_id)` desc)
- [x] **3.2** Filters: time range, agent, has-errors (match current list contract)
- [x] **3.3** No lake fallback on Postgres: list always `session_summary` (empty → empty). Postgres ⇒ summary always on; sqlite keeps lake as sole store
- [x] **3.4** Detail paths unchanged (still `traces` / lake)
- [x] **3.5** API / Explorer contract tests: list p95 independent of span volume in window
- [x] **3.6** **[P]** Update Explorer/docs for summary-backed list behavior
  - Evidence: `sp-llm/apps/explorer/design/list-and-shell.md` (summary-backed list); `session-list-summary.md` §7.3; `api.test.ts` asserts no obs scan on list path

---

## Stage 4 — Rebuild (heal + ops)

Depends on **A** + **1** (+ ideally **2** so UPSERT path is shared). Can start **in parallel with Stage 3** if reduce UPSERT helpers are extracted.

- [x] **4.1** Shared reduce/rebuild aggregation helper (DRY with Stage 2)
  - Evidence: `reduce_sql::compile_session_summary_aggregate_sql`; `aggregate_sessions_from_lake` / `upsert_summary_rows` / `rebuild_tenant_window`
- [x] **4.2** `SessionSummaryRebuildJob`: leased `session_summary.rebuild`; windowed re-aggregate from `traces`
  - Evidence: `job.rs` `SessionSummaryRebuildJob`; registered in `compaction/scheduler.rs`
- [x] **4.3** Periodic lookback = `max_reduce_span_seconds`; cadence `rebuild_interval_ms` default 24h (Q #4)
  - Evidence: config defaults + RebuildJob `run` window
- [x] **4.4** Ops: `POST /v1/llm/sessions/summary/rebuild` `{from,to}` → lease + sync rebuild; reject inverted/oversized
  - Evidence: `query::rebuild_session_summary`; route in `api/mod.rs`
- [x] **4.5** Test: truncate `session_summary` → rebuild restores list; Parquet untouched
  - Evidence: `truncate_summary_rebuild_restores_list_parquet_intact` e2e

---

## Stage 5 — Promote hot list columns → **promotion invariant close-out**

Depends on **2**/**3** working.

### LOUD RULES (non-negotiable — fail the PR if violated)

1. **Tests first / all cases.** No “add tests later.” Evidence must include positive *and* negative cases (bag-only must NOT fill reduce fields).
2. **DRY hard.** One source for attr keys (`models/attr_keys`), one required-col list (`REQUIRED_TRACES_HOT_COLS` / yaml), one reduce SQL builder. **No duplicated logic. No string literals or magic numbers in new code — constants only.**
3. **Simplicity is king.** **Never** add features, fallbacks, backward-compat shims, or second write paths unless the design explicitly requires them. Prefer closing the stage over inventing work.
4. **No rubber stamps.** Review must aggressively reject busywork and scope creep.

### Research verdict (2026-09-19)

Stage 5’s original wording (“promote so reducer prefers typed over MAP”) was **executed in Stage 2**:
- `reduce_sql` is promoted-only (hard ban on `attributes` / MAP).
- `traces-query-hot-attrs.yaml` auto-activates on postgres scope ensure.
- List filters already hit typed `session_summary` columns.
- `agent_name` is **auth column** or agent-observation `message_type` — bag `sp.agent.name` is ignored by reduce **by design** (not a missing promotion).

### Approaches (pick one)

| | Scope | Verdict |
|---|---|---|
| **A. Close-out + regression locks** | Docs truth + aggressive evidence tests only; **no production behavior change** unless a test fails | **Recommended** |
| **B. Ingest bag→`agent_name` when auth empty** | Copy `sp.agent.name` into typed col | **Rejected by critic** — unrequested fallback; duplicates agent semantics |
| **C. Also fill `user_id` from `enduser.id`** | Widen product-hot | **Rejected by critic** — contradicts yaml “enduser.id stays bag-only”; new feature |

### Revised tasks (Option A)

- [x] **5.1** Doc truth: Stage 2 already did promote-over-MAP; agent = auth/`message_type` not bag; list = typed summary cols
- [x] **5.2** Evidence matrix (all must pass):
  - reduce SQL contains **no** `attributes` / `resource_attributes` / bag key literals (`attr_keys` constants in asserts)
  - `REQUIRED_TRACES_HOT_COLS` ⊆ canonical yaml (single source via `llm_promo().reduce_required_cols()`)
  - postgres ensure path activates yaml when required cols missing
  - **Negative:** MAP-only tokens/user/model/agent → summary fields stay NULL/0 (prove no secret MAP extract)
  - **Positive:** auth `agent_name` and agent `message_type` still win (reuse accuracy fixtures; don’t duplicate)
  - list SQL predicates stay typed-only (existing lock)
- [x] **5.3** Explicit **non-goals** in docs: no ingest bag→column copy; no `enduser.id` promotion; no `sp.agent.name` in hot-attrs yaml

**Critic:** [Stage 5 harsh review](4c260b31-d164-4da5-ba33-68ec5e3d100a) — rank A ≫ kill B/C; remaining real product work is Stage 4 + V, not MAP crutches.

**Stage 5 notes (2026-09-19):** Close-out only — DRY required cols from `llm_promo`; yaml keys locked to `attr_keys`; MAP-ignore + ensure-activate evidence tests.

---

## Verification gate (before calling the work ready)

Depends on Stages **0**, **A**, **1–4** (and **0b** if compilers changed).

- [x] **V.1** Success criteria in `session-list-summary.md` §15 checked with evidence
  - List ≠ f(spans): `http_session_summary_list_independent_of_span_volume`
  - Detail lake-only: `http_session_detail_still_reads_lake_after_summary_reduce`
  - Truncate→rebuild: `truncate_summary_rebuild_restores_list_parquet_intact`
  - No Explorer obs scan: `api.test.ts` + list-and-shell.md
  - Reduce accuracy: `reduce_accuracy_tests::*`
  - Lease shared module: Stage A + reduce/rebuild on `spawn_runner`
- [x] **V.2** Success criteria in `async-jobs.md` §12 checked for Stage A (multi-replica lease + MemoryLeaseStore); criteria 2–4 wait on Stage C
- [x] **V.3** Dirty UPSERT rate ≈ lake flush rate, not span rate
  - Evidence: dirty after coalesce flush only (`http_session_summary_*` + dirty mark-after-commit tests); flush 0 and >0 share coalesce path
- [x] **V.4** No reducer/rebuild SQL without `record_date` + timestamp bounds
  - Evidence: `reduce_sql::tests::{reduce_sql_has_pushdown_and_no_attributes,rebuild_sql_window_wide_no_in_list}`
- [x] **V.5** Workspace / thelake test gate green for touched crates
  - Evidence: `cargo fmt` + `clippy -D warnings` + `cargo test --lib session_summary` + `session_summary_list` e2e (8/8)

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
