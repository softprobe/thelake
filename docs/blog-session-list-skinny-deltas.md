# Fast session lists without becoming ClickHouse

**Subtitle:** How Softprobe keeps AI evidence cheap forever while making Explorer’s session list efficient, maintainable, and flexible.

---

Explorer’s Sessions page has a simple job: show a triage list — which sessions happened, which agent, roughly how heavy, whether anything looked broken — then let you open one and reconstruct the full recording.

The data plane underneath that page is not simple. Softprobe Runtime (thelake) stores production AI traces as durable evidence in DuckLake: open Parquet, long retention, customer-controlled storage. A single coding-agent session can contain hundreds of spans with prompts, tool payloads, and events. The list view never needed any of that body data. Until recently, it still paid for it.

This post explains the problem, the designs we rejected, and the approach we chose: **thin lake indexes with merge-on-read skinny deltas**, configured like our existing promotion manifests.

## The product contract

Softprobe’s positioning is deliberate:

> Preserve AI traces as durable assets. Query them with standard SQL. Do not reduce them to short-lived dashboards.

That implies:

- **Evidence stays forever** (or for as long as the customer wants) in open files.
- **Acceleration is pruning and promotion**, not a second source of truth you must delete to stay cheap.
- The decisive workflow is *find a small set of sessions, then fetch bodies* — not “subsecond multi-tenant dashboard concurrency over months of rollups.”

Session **detail** already matches that shape: bound by `session_id`, load observations for that session. Session **list** did not.

## What made the list expensive

On the happy path, Explorer called `POST /v1/llm/sessions/search`, which aggregates over `union_spans` for the whole time window, then applies `LIMIT`. Aggregation happens **before** the cut: every span in “last 7 days” participates, including attribute bags and event payloads the list never displays.

Worse, the UI then ran a second window-wide `observations/search` scan to rewrite counts so list numbers matched detail’s dedup and primary-error rules. Two lake scans for a table that only needs a handful of summary fields.

When filters forced a client-side fallback, the browser pulled pages of observations and grouped them locally. Correct-ish, catastrophic at scale.

**Diagnosis:** the list was using the evidence plane as a directory. Those are different jobs.

## Designs we rejected (and why)

### Materialized session rollups as source of truth

Maintain a `session_summaries` table (or ClickHouse-style AggregatingMergeTree) updated on every change. List is fast. You also inherit:

- a second truth that must stay fresh;
- backfill and repair stories;
- pressure to **delete or expire aggregates** to control cost — exactly the economics Softprobe exists to avoid.

Rollups are fine for metrics dashboards. They are the wrong hill for an evidence lake.

### Per-span upserts into Postgres

Explorer already has Supabase for workspaces, agents, and findings. Putting a session directory there is tempting for joins. Updating it on every span turns Postgres into the new ingest bottleneck: row locks, WAL, index churn. We would have moved the fire, not put it out.

Postgres plugins do not fix this. Shared buffers coalesce *pages*, not *logical session merges*. There is no production “memory coalesce UPSERT into one session row” extension we would bet the product on.

### Redis / streaming MVs / “flexible stats engines”

Redis as a coalesce buffer is fine as an optional absorber; as durable stats, it is another ops plane. RisingWave, Materialize, and Flink give excellent live aggregates — and a second cluster, state store, and dual-write story. `metrique-aggregation`-style crates are compile-time metric merges for EMF-like sinks, not runtime YAML driving DuckLake directories. OTel Collector `aggregate_on_*` runs outside our SoT.

None of that matches “one lake, no new ops surface.”

## The design we chose

Three ideas, stacked:

### 1. Skinny deltas in the same lake

On each span batch write, derive **one small row per `session_id` present in that batch** and append it to `session_stats_delta`:

- timing (`start` / `end`);
- summable counts (`observation_count`, `error_count`, tokens, cost);
- cheap dimensions (`agent_name`, nested-child flag);
- no `attributes`, no `events`, no prompts.

Many batches ⇒ many delta rows for the same session. That is intentional.

### 2. Merge-on-read (steal the ClickHouse idea, stay on Parquet)

List query:

```sql
SELECT session_id,
       MIN(start_time), MAX(end_time),
       SUM(observation_count), SUM(error_count),
       SUM(total_tokens), SUM(total_cost),
       …
FROM session_stats_delta
WHERE /* day/time bounds */
GROUP BY session_id
ORDER BY …
LIMIT …
```

We borrowed ClickHouse SummingMergeTree’s *shape* — append partials, combine later — without running ClickHouse. Compaction is the lake’s existing maintenance. Spans remain the source of truth; deltas are rebuildable.

### 3. Configurable measures without a new product

We cannot predict every future list filter. We also refuse a free-form analytics engine. The compromise is the same governance Softprobe already uses for hot attributes: a **declarative manifest** (`softprobe.session_stats.v1`).

- Builtin defaults cover today’s Explorer list.
- Extra numeric measures live in a `measures` map and merge with `sum` / `min` / `max` / `any`.
- New filter dimensions prefer **promotion on traces first**, then declaration on the delta — additive, reviewable, no silent schema sprawl.

Undeclared filters return **400** — declare the dimension on
`softprobe.session_stats.v1` (and apply) before the list can use them.
Configurability makes *adding the declaration* cheap; it does not invent
indexes for unknown fields.

```text
OTLP batch
  ├─► traces          (fat evidence, forever)
  └─► session_stats_delta  (skinny partials, same ingest path)

Explorer list  ──► GROUP BY session_id over deltas
Explorer detail ──► traces / observations by session_id
```

No new service. No Redis. No Postgres hot path. If delta write fails, span
ingest still succeeds; those sessions are absent from the list until deltas
exist (re-ingest or operator rebuild). List never scans `union_spans`.

## Why this is efficient

**Bytes.** List never opens event payloads or attribute bags. Parquet column prune and day partitions do the work they were designed for.

**Write amplification.** Cost is O(sessions in the batch), not O(spans) of UPSERTs. A 500-span agent turn might add one delta row, not 500 Postgres updates.

**Read amplification.** `LIMIT` applies after merging *directory rows*, not after aggregating every span in seven days. Page size dominates cost, not window size — as long as the window’s delta files stay small, which they do relative to traces.

**Two planes.** Detail stays exact and expensive by nature (you asked for the recording). List stays approximate and cheap by nature (you asked for triage). We stopped paying detail’s correctness tax on every list paint.

## Why this is easy to maintain

**One storage system.** Deltas live next to traces under the same DuckLake scope, partition, and compaction story operators already run.

**No second lifecycle.** We do not schedule “delete old session rollups to save money.” Evidence retention policy stays the customer’s; the directory is tiny.

**Failure isolation.** Span write is sacred. Delta write is best-effort.
Ingest SLOs are not coupled to directory freshness. List requires deltas —
no silent span-scan fallback.

**Familiar ops.** Manifests follow the promotion apply/load pattern. Engineers who already reason about hot columns can reason about list measures.

**Honest semantics.** List `error_count` is “sum of ERROR statuses in batches,” not “primary-error tree after dedup.” Documented divergence beats silent double scans that try to make two different truths look identical.

## Why this stays flexible

**Core columns** lock today’s API (`SessionSummary`) so Explorer does not churn.

**Manifest extras** let product add mergeable measures without redesigning the table every time.

**Promotion + dimensions** give a path for new filters: accelerate the field on ingest, then expose it on the directory.

**Schema gate.** Undeclared list filters and a missing `session_stats_delta`
table return 400. Historical windows without deltas return an empty list
until rows are rebuilt or re-ingested — not a fat span scan.

What we still will not do: pretend every ad-hoc investigation filter belongs on the list path. Unbounded flexibility is how you recreate the original full-lake scan. Softprobe’s bet is progressive usefulness — declare what matters, keep everything else in the evidence plane until you need it.

## What we are not claiming

Until we publish benchmarks, we claim architecture, not “faster than ClickHouse.” The credible question is narrower:

> For selective, intermittent session triage over long-lived customer-owned evidence, can file pruning plus skinny merge-on-read deltas deliver acceptable interactive latency at materially lower total retention cost?

That is the Softprobe question. This design answers it without abandoning the thesis.

## Takeaway

Fast session lists do not require a second analytics database. They require respecting the split Softprobe already believes in:

- **Directory** — find sessions with almost no bytes.
- **Evidence** — fetch bodies only after you know which session you care about.

Skinny deltas, merge-on-read, and a promotion-style stats manifest give us efficiency at scale, almost no new ops, and room to grow list fields without competing with ClickHouse on rollup economics — or with Postgres on per-span upserts.

Evidence stays cheap. The list stays light. Detail stays complete.
