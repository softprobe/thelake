# Event-time layout — implementation tasks

**Design:** [`design-event-time-layout.md`](./design-event-time-layout.md) + [`design-sql-and-schema.md`](./design-sql-and-schema.md)  
**Issue:** [#73](https://github.com/softprobe/thelake/issues/73)  
**Process:** Slices from the one-clock plan. Each slice: TDD → `make ci` (or scoped tests) → **hostile review (coding-rules brief)** → fix → merge.  
**Rules:** DRY. No dual-read / feature flags / compat shims. No `record_date` / `window_ts`.

Delivery follows [design-sql-and-schema.md](./design-sql-and-schema.md) §3. Metrics layout doc rewrite is a late slice.

---

## Decision → test matrix

| Decision | Must-have tests |
|----------|-----------------|
| D1 One clock / no day columns | Schema asserts no `record_date` / `event_date` / `window_ts`; partition expr on `timestamp` |
| D2 Query = `timestamp` bounds only | Recipes: timestamp bound present; no forbidden time column tokens; no dual day+timestamp predicates |
| D3 Clean cutover | No dual-read cfg; rollout notes say reset/copy |
| D4 No optional bounds | `push_optional_time_bounds` absent; no `Option` on lake windows |
| D5 TIMESTAMP_NS | Type unify tests |
| D6 Session detail = summary window | Handler ignores list range |
| D7–D8 Sort / scores | `SORTED BY` + ORDER BY match; scores in layout |
| D9 / D12 Gate + locality | `ensure_fact_scan_bound` (D12) unit tests; SQL only under `src/sql/` |
| D10 Inline 500 | Config assert |
| D11 Observations payload | Projection unit assert |

---

## Slices (map to plan)

| Slice | Goal |
|-------|------|
| 0 | Doc lock (this file + event-time design aligned) |
| 1 | Greenfield EXPLAIN: day(`timestamp`) prune without `record_date` |
| 2 | `src/sql/` foundation (`QueryWindow::bind_*`, gate, literals) |
| 3 | Move recipes into `src/sql/`; locality hard-fail |
| 4 | Schema/writer drop `record_date` / rename `window_ts` → `timestamp` |
| 5 | Wire gate on all execute paths |
| 6 | Copy/flip; rewrite `metrics-timeseries-layout.md` |

### Hostile review gate (every slice)

```
Attack this change. No rubber stamp.
1. DRY — duplicated logic?
2. Simplicity — compat/fallback/flag not in design?
3. Tests — missing/weak?
4. Gates — make ci / scoped tests green? Cite evidence.
Return: critical findings, then residual risks.
```

### PR checks

`make ci` before merge (scoped `cargo test` only for pure unit mid-slice, then full ci).
