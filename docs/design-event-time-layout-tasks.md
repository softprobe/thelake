# Event-time layout — implementation tasks

**Design:** [`design-event-time-layout.md`](./design-event-time-layout.md)  
**Issue:** [#73](https://github.com/softprobe/thelake/issues/73)  
**Process:** Multiple PRs (phases). Each phase: implement → aggressive tests → **hostile review (subagent, inherit/auto)** → fix → PR must pass all checks.  
**Rules:** DRY (one helper, one writer day assignment). No dual-read / feature flags. No phase leaves optional lake time bounds as a supported API.

Delivery phases are **PR slices**, not dual-model eras. After Phase 2, no OTLP LLM compiler may omit day+timestamp bounds.

---

## Decision → test matrix (every decision, no exception)

| Decision | Must-have tests | Phase |
|----------|-----------------|-------|
| **D1** Keep DATE identity partition (no year/month/day transforms) | Assert `otlp_layout` / scores layout SQL contains `PARTITIONED BY (record_date)` and **does not** contain `year(timestamp)` / `month(timestamp)` / `day(timestamp)` | P4 |
| **D2** Single writer assigns `record_date = timestamp.date_naive()` | (a) Unit: `partition_day_from_event_time(ts)` is the only public setter; (b) grep/static test or `#[deny]`-style test that Score/Arrow paths call it; (c) property: assigned day == `ts.date_naive()` | P4 |
| **D3** Callers never pass partition day | Helper API accepts only `QueryWindow { from, to }`; no `record_date` / `event_date` params on public compile fns — compile-fail or API shape unit tests | P1 |
| **D4** Helper always emits day + timestamp, same window | Unit tests: SQL contains `record_date BETWEEN DATE` **and** timestamp bounds; days derived from `from`/`to`; inverted window errs; predicate order day before timestamp | P1 |
| **D5** Delete `push_optional_time_bounds` | Test that symbol is gone (`rg` in CI test or `include_str!` absence); no `Option<DateTime>` on OTLP lake compilers | P1–P2 |
| **D6** Helper owns cast form | Only helper emits timestamp compare SQL; reduce/LLM tests assert identical literal/cast shape | P1 |
| **D7** Session detail = summary start/end only, pad 0 | Handler/integration: client `from`/`to` ignored for lake SQL; window = summary; missing summary → 404; pad not applied | P3 |
| **D8** Sort keys + score sort lead | Layout SQL asserts exact `SORTED BY` strings; writer ORDER BY matches; score write with empty session+trace rejected or deterministic lead column tested | P4 |
| **D9** No `app_id` sort lead | Assert sorted_by does **not** start with `app_id` | P4 |
| **D10** Scores in otlp_layout | `OTLP_LAYOUT_TABLES` includes scores; ensure applied on create | P4 |
| **D11** No dual-read / flags | Rollout notes / this tasks doc: no “support both”; CI test forbids `cfg` feature for dual layout | P1 (doc) + each PR description |
| **D12** No execute SQL regex guard | No new execute-path predicate parser; enforcement via typed compile + unit tests only | P1 (explicit non-goal test: guard module absent) |
| **D13** Inline default 500 | Config default / yaml assert `data_inlining_row_limit == 500` | P6 (or P1 if cheap) |
| **D14** Session observations include attributes/events | `compile_session_observations_sql` projects payload; unit assert; product Explorer trajectory needs `sp.input`/`sp.output` | P6 |

### Acceptance → phase

| AC | Phase |
|----|-------|
| AC1 One writer day assignment | P4 |
| AC2 Every OTLP `compile_*` asserts day+timestamp; no Option time | P2 (LLM), P5 (compat/telemetry) |
| AC3 `push_optional_time_bounds` deleted | P1–P2 |
| AC4 Session detail ignores list range | P3 (+ Explorer follow-up PR if needed) |
| AC5 otlp_layout covers 3 tables; ORDER BY = SORTED BY | P4 |
| AC6 EXPLAIN fixture committed | P6 |
| AC7 session-list-summary hard rule rewritten | P6 (partial P1 pointer OK) |
| AC8 No dual-read in rollout | Every PR description + this doc |

---

## Phase 1 — QueryWindow foundation (PR)

**Goal:** One DRY helper; reduce uses it; optional bounds API deleted or unreachable.

### Tasks

1. Add `src/api/query_window.rs` (or fold into `sql_support.rs` if smaller — prefer one module name used everywhere):
   - `QueryWindow { from, to }`
   - `QueryWindow::try_new` → err if `from > to` (max-span stays at rebuild/job callers — not on `QueryWindow`)
   - `fn push_otlp_time_predicates(conditions, window, identity)` — **only** emitter of day + event-time bounds (hardcoded `record_date` / `timestamp` column names).
2. Refactor `reduce_sql.rs` to call the helper (delete duplicated date/timestamp string building).
3. Delete `push_optional_time_bounds` (or make it `#[deprecated]` private and unused — prefer **delete**).
4. Update any callers of optional bounds that break — temporary: force required `from`/`to` on those compile fns (if that blows Phase 1 scope, stub compile fns to require window and fix call sites in P2 **in the same PR** if few). Prefer **same PR** so main never has half-deleted optional API with broken call sites.

### Tests (aggressive)

- `query_window` unit: inverted, equal day, multi-day.
- Emitted SQL snapshot/contains: `record_date BETWEEN`, timestamp bounds, order (day index < timestamp index).
- `reduce_sql` tests still pass; assert they use same substrings as helper (DRY: one expected fragment builder in tests).
- `src/`-wide absence of `push_optional_time_bounds`.
- D3: `QueryWindow` fields are only `from`/`to`; push uses fixed column constants.
- D6 for this phase: converted compilers + reduce emit via `push_otlp_time_predicates` only (remaining LLM hand-rolls = Phase 2).

### Hostile review gate

Subagent hostile review on branch diff before PR. Must address all fatal findings.

### PR checks

`cargo test` (scoped + full CI). PR links #73.

---

## Phase 2 — All LLM OTLP compilers on helper (PR)

**Goal:** AC2 for `api/llm/query.rs`; AC3 complete.

### Tasks

1. Every `compile_*` touching `traces` / `scores` in LLM query uses `QueryWindow` + helper.
2. `DetailQuery.from/to` become **required** (not `Option`).
3. `compile_scores_for_span_sql` gains required window.
4. Search / session / observation / recording / trace compilers: required window already or tighten.

### Tests

- Parametric test over **all** LLM compile entry points: each SQL has day+timestamp bounds.
- Explicit test: no compile fn signature with `Option<DateTime<Utc>>` for lake scans (inventory list in test).
- Existing LLM query unit tests updated.

### Hostile review → PR → CI

---

## Phase 3 — Session detail owns summary window (PR)

**Goal:** D7, AC4.

### Tasks

1. `get_session` / `get_session_observations` / `get_session_recording`: resolve window from `session_summary` by `session_id`; **ignore** query `from`/`to` for lake predicates (may keep query params for OpenAPI compat but unused — hostile review may demand **remove** unused params; prefer remove from contract if Explorer can change in same milestone).
2. 404 if no summary row.
3. Pad = 0.
4. Explorer (`sp-llm`): stop sending list range for detail — **sibling PR** if API removes params; if thelake ignores params, Explorer cleanup can be same milestone second PR.

### Tests

- Unit/integration: summary `(t0,t1)` → compiled/executed window equals `(t0,t1)` even if request sends 30d.
- Missing summary → 404.
- Pad 0: exact summary timestamps in SQL literals.

### Hostile review → PR → CI

---

## Phase 4 — Layout + writer single day assignment (PR)

**Goal:** D1, D2, D8, D9, D10, AC1, AC5.

### Tasks

1. `otlp_layout.rs`: sort keys per D8/D9; add scores table.
2. Writer `ORDER BY` aligned.
3. `partition_day_from_event_time` single function; Arrow + Score write paths call only it.
4. Score model: no independent `record_date` set without going through that fn (constructor).

### Tests

- Layout SQL golden strings.
- Writer order matches.
- AC1: search test or unit that only one assignment path exists.
- D1: no year/month/day(timestamp) in partition SQL.

### Hostile review → PR → CI

**Note:** Existing catalogs need reset to pick up new `SORTED BY` — document in PR (D11).

---

## Phase 5 — Telemetry + Tempo + Loki (PR)

**Goal:** AC2 remainder.

### Tasks

1. Telemetry search/details/field_values: required time window (field_values must gain from/to or die).
2. Tempo `trace_scan_sql`: required start/end.
3. Loki scan: required bounds (validate_time_range already — make empty window impossible).

### Tests

- Each compat/telemetry compile path in parametric day+timestamp (or timestamp+derived day for logs if `record_date` present) inventory.
- field_values without window → 400.

### Hostile review → PR → CI

---

## Phase 6 — Docs, EXPLAIN, inline assert, skinny projection (PR)

**Goal:** D13, D14, AC6, AC7, AC8.

### Tasks

1. Rewrite `session-list-summary.md` hard rule → point at design + QueryWindow.
2. Commit EXPLAIN fixture / integration proving unrelated days not listed (AC6).
3. Assert inline default 500 (D13).
4. `compile_session_observations_sql` **includes** attributes/events (product Explorer trajectory). Search/list may stay skinny; expand-on-demand remains for debug SessionDetailView.
5. Open metrics follow-up issue with kill date (not eternal carve-out).
6. Retitle #73.

### Tests

- Doc link presence not tested; code: projection test; inline default test; EXPLAIN/integration test.

### Hostile review → PR → CI

---

## Hostile review protocol (every phase)

Launch **one** `generalPurpose` subagent, model **inherit** (auto), `run_in_background: false`, with prompt:

```text
You are a HOSTILE principal engineer. Simplicity is the top rule.
Full Repository Path: /Users/bill/src/arex/thelake
Diff: branch changes
Custom Instructions: Review against docs/design-event-time-layout.md and docs/design-event-time-layout-tasks.md for THIS phase only. Fail the phase if: dual clocks, optional time bounds remain, missing tests for any decision claimed in the phase, DRY violations (duplicated predicate builders), execute-SQL regex guards, dual-read/flags, or acceptance criteria for this phase unmet. Be ruthless. Sections: Verdict; Fatal; Footguns; Missing tests; Required fixes before PR.
```

Do not open PR until Verdict is **ship** or fixes landed and re-reviewed if fatal.

---

## PR hygiene

- Feature branch per phase: `feat/event-time-pN-…`
- Do not include unrelated dirty files on `main`.
- Each PR: Summary + Test plan mapping decisions/AC → tests.
- Wait for CI green; fix failures before merge.
- After merge, next phase branches from updated `main`.

---

## Metrics follow-up (not a phase here)

Issue: apply §0 rule 1 to metrics with an explicit kill date — separate from OTLP PRs.
