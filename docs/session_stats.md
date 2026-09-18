# Session list skinny deltas (`softprobe.session_stats.v1`)

**Status:** Current  
**Spec version:** `softprobe.session_stats.v1`  
**Apply API:** authenticated `POST /v1/promotions/apply` (same endpoint as telemetry promotions)  
**Canonical product fixture:** [`session_stats/default.yaml`](session_stats/default.yaml)

This is the contract for Explorer session-list merge-on-read stats. Promotion
column docs live in [`promotion.md`](promotion.md); this file covers only
session stats manifests.

## What problem this solves

Each ingest batch appends one skinny row per `session_id` to
`session_stats_delta`. `POST /v1/llm/sessions/search` **only** merges those
rows with `SUM` / `MIN` / `MAX` / `any_value`. There is no `union_spans`
fallback for the list path.

Configurability reuses promotion governance: apply a YAML manifest, persist an
active `promotion_specs` row with `target_kind = session_stats`, and resolve
that manifest (or the builtin default) on **both** write and list.

## Builtin vs active

| Source | When used |
|--------|-----------|
| [`docs/session_stats/default.yaml`](session_stats/default.yaml) | Compiled into the binary via `include_str!`; used when no active `session_stats` row exists |
| Active `promotion_specs` row | After `POST /v1/promotions/apply` with `specVersion: softprobe.session_stats.v1` |

Apply supersedes prior active `session_stats` specs for the same scope
(`target_tables = session_stats_delta`). Softprobe does **not** backfill
historical delta rows when the manifest changes.

## Ops and storage

Allowed measure ops: **`sum`**, **`min`**, **`max`**, **`any`**. Ops such as
`uniq` or quantiles are rejected at parse/apply time.

| Kind | Storage |
|------|---------|
| Core measures (`observation_count`, `error_count`, tokens/cost, `trace_count`, `start_time`, `end_time`) | Typed columns on `session_stats_delta` |
| Map-backed measures (any non-core measure name) | `measures` `MAP(VARCHAR, DOUBLE)` — **no** new typed column on apply |
| Physical dimensions (`agent_name`, `is_nested_child`) | Already typed columns |
| Additional filter dimensions (e.g. `model_name`) | Apply runs `ALTER TABLE session_stats_delta ADD COLUMN IF NOT EXISTS … VARCHAR` |

## Apply

```http
POST /v1/promotions/apply
Authorization: Bearer <tenant-token>
Content-Type: application/json
```

```json
{
  "manifestYaml": "specVersion: softprobe.session_stats.v1\n..."
}
```

Success shape:

```json
{
  "specVersion": "softprobe.promotion.apply.v1",
  "applied": true,
  "target": { "kind": "session_stats", "tables": ["session_stats_delta"] },
  "schemaChanges": [
    { "table": "session_stats_delta", "action": "map_measure", "column": "tool_calls" },
    { "table": "session_stats_delta", "action": "add_column", "column": "model_name", "type": "string", "nullable": true }
  ]
}
```

- Core measures/dimensions that already exist → no DDL entry (or no-op).
- Map-backed measures → `action: map_measure` only.
- New dimension columns → idempotent `ADD COLUMN IF NOT EXISTS`.

## List vs detail errors

List `error_count` is the merge-on-read **sum of per-batch ERROR status
counts**. Session detail primary-error topology is a different signal. Both
endpoints may succeed while those numbers disagree — clients must not treat
list `error_count` as detail primary-error equivalence.

## Filters and schema gate

Before SQL generation, `POST /v1/llm/sessions/search`:

1. Resolves the active (or builtin) `softprobe.session_stats.v1` manifest.
2. Rejects filters that are not declared dimensions on that manifest.

Delta-compatible filters today: time window, `has_errors`, `roots_only`
(nested-child flag), plus any key in `dimensions` that is a physical column
or a declared session_stats dimension (e.g. `agent_name` on the builtin
manifest; `user_id` / `model_name` after apply). Otherwise the API returns
**400** with
`session_stats_delta table schema does not allow this query: …`.

If `session_stats_delta` is missing, the list SQL fails and is mapped to the
same **400** schema error (no pre-flight probe). Empty delta rows in-window
return an empty list — not a span scan.

Delta write failures remain best-effort on ingest (spans still commit). Without
delta rows, those sessions simply do not appear on the list until rebuilt or
re-ingested.
