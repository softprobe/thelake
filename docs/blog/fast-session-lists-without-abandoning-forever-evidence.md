# Fast session lists without abandoning forever evidence

*How Softprobe keeps agent session triage snappy while the lake stays the single source of truth.*

---

Every coding agent session leaves a trail: tool calls, model turns, errors, tokens, timing. That trail is gold when something goes wrong — but only if you can **find the right session quickly** and still **open the full evidence** without paying observability-tax forever.

That tension is the product problem Softprobe is built around.

This post is about a design change in our telemetry store (**thelake**): how we make the Explorer **Sessions** list cheap, without turning Softprobe into yet another metrics warehouse that discards or dual-writes the truth.

---

## What Softprobe is optimizing for

Softprobe Agent QA is not trying to be a second Datadog.

We care about:

1. **Capture once, keep forever (cheap).** Full OTLP traces and logs for agent runs live in a lake-backed store. Evidence is not a temporary cache.
2. **Ask bounded questions.** Humans and AI agents triage sessions, open one session, and dig into observations — not build real-time multi-tenant dashboards over every span ever written.
3. **Stay simple.** No Kafka-shaped summary fleet, no ClickHouse-shaped rollup parallel universe, no “summary table in the lake *and* another copy elsewhere” that drifts.

If that sounds boring, good. Boring storage is how you keep agent QA affordable at scale.

---

## The UX that broke under growth

Explorer’s Sessions list is a triage surface:

- Which sessions ran in this window?
- Which agent?
- How many steps? Errors? Tokens / cost?
- Filter → page → open detail.

Detail is easy in principle: read **one session** from `traces`. Cost scales with that session.

List was the expensive path. On the original design, every list load effectively meant:

> *Aggregate all spans in the time window, group by `session_id`, then page.*

That works for demos. It does not work when a team runs agents all day. List latency and cost started tracking **spans in the window**, not **rows on the page**.

So we had a classic product fork:

| Temptation | Why it fails Softprobe |
|---|---|
| Drop old spans / shorten retention | Breaks “forever evidence” |
| Dual-write a lake `session_facts` table | Two derived truths; compaction and ops get harder |
| Push a counter into the UI / Explorer DB | Explorer does not own ingest; multi-instance breaks |
| Stand up ClickHouse / Elastic “for lists” | New system, new cost center, new drift |

We wanted list cost ≈ **page size × filter selectivity**, while detail still reads the lake.

---

## The design we chose

**One skinny directory in catalog Postgres. Evidence stays only in DuckLake.**

```text
ingest (every replica)
  → commit coalesced batch to DuckLake `traces`
  → ONE batched dirty UPSERT for distinct session_ids in that batch

async reducer (lease winner only)
  → time-scoped GROUP BY from `traces`
  → UPSERT session_summary
  → DELETE / ack dirty rows

list   ← session_summary     (fast triage)
detail ← traces            (full evidence)
```

### Evidence vs summary

| Data | Lives in | Role |
|---|---|---|
| Span bodies, attrs, events | DuckLake `traces` | **Source of truth** |
| Triage fields for the list | Postgres `session_summary` | Derived, **rebuildable** |
| “These sessions changed” | Postgres `session_summary_dirty` | Short-lived work queue |
| Who runs the reducer | Postgres job lease | Coordination across replicas |

If the summary burns down, you rebuild from `traces`. You do **not** lose sessions. That is non-negotiable for Softprobe branding: the lake is the product’s memory; the summary is a cache with schema.

### Why not maintain absolute counters in memory?

Crashes, restarts, and multiple ingest replicas make process-local counters a lie. Softprobe’s rule is sharper:

> **All summary numbers come from re-aggregating `traces` over a time scope.**  
> Dirty rows only say *which sessions to refresh*, not *what the counts are*.

Late spans are fine: touch dirty → next reduce replaces the row. No fragile OPEN/FINALIZED state machine in v1.

---

## Batching is part of the product contract

A dirty table that UPSERTs **per span** would hammer Postgres harder than the list problem we set out to fix.

Ingestion is already batched (collectors + Softprobe soft coalesce). So we lock the contract:

- Dirty write happens **once per successful lake flush batch**, after folding `session_id → {min_ts, max_ts}` in memory.
- Enabling `session_summary` requires soft coalesce (`flush_interval_seconds > 0`). Flush-through mode stays available when you do not need the summary path — but we refuse the footgun of “summary on + write-through every OTLP request” as the default production shape.

Coalesce is not an afterthought. It is how session summary stays aligned with how the lake already wants to be written.

---

## Reduce as a leased async job — not on the hot path

Ingest must stay fast and boring: write Parquet/DuckLake, mark dirty, return.

The reducer runs on the **same async job runner** we use for lake maintenance (compaction, metadata), under a **Postgres lease** so only one replica reduces a given tenant at a time.

That gives us:

- No double-scan races between replicas
- No second “session-summary microservice”
- One ops mental model: *jobs + leases*

And the SQL always carries a **mandatory time window** (`record_date` + timestamp bounds) so DuckLake can prune Parquet files. A reducer that scans “all time for these session_ids” would recreate the original problem in the background.

After a successful UPSERT, dirty rows are deleted with a snapshot guard (`updated_at <= batch_snapshot`) so a newer touch that arrived mid-reduce is not lost.

---

## What this means for users

**Before:** opening Sessions could feel like querying your entire telemetry window.

**After:** Sessions feels like a directory — filter, page, open — while the full observation stream remains one click away in the lake.

You get:

- **Faster triage** as agent volume grows  
- **Unchanged deep dive** into prompts, tools, and errors  
- **Retention without rollup theater** — keep cheap forever evidence, rebuild the directory whenever you need  

That is the Softprobe shape: observability for **agent QA**, not a second APM empire.

---

## How the design evolved (and what we refused)

Designs do not arrive fully formed. Ours went through several wrong-but-tempting shapes:

1. **Explorer-owned directory** — rejected; Explorer is read-only for this path; ingest lives in thelake.  
2. **Dual DuckLake session summary table** — rejected; one derived store is enough; lake stays evidence-only.  
3. **In-memory touch set as source of dirty truth** — rejected under multi-replica; dirty must be durable.  
4. **Inline reduce on every ingest** — rejected; list freshness of a few seconds beats putting aggregation on the write path.  
5. **Per-span dirty UPSERT** — rejected; batch with coalesce or do not enable the summary.

Each refusal protects the same brand promise: Softprobe stays **simple enough to operate** and **honest about where truth lives**.

---

## Why this matters beyond one screen

Agent systems are getting denser. More tools, more retries, more tokens per “session.” Teams that treat every span as a dashboard metric will pay warehouse prices for QA workflows that only needed a **good list** and a **faithful detail**.

Softprobe’s bet:

> Keep the evidence. Summarize the directory. Lease the work. Rebuild when life happens.

If you are building or evaluating agent products and want session review that stays fast as volume grows — without renting a second observability stack — this is the direction we are shipping.

---

## Try Softprobe Agent QA

- Explore sessions and findings in **Softprobe Explorer**  
- Capture with OpenTelemetry into Softprobe’s lake-backed runtime  
- Dig into full traces when a session misbehaves — the evidence is still there  

Questions, feedback, or “we hit the same list-vs-lake wall” stories: we want them. Agent QA is a young category; the teams that share design constraints early help define it.

**Softprobe** — forever-cheap evidence. Fast enough to triage.

---

*Engineering note: this describes the session-summary design for thelake (catalog Postgres `session_summary` + durable dirty queue + leased async reducer). Implementation lands behind shared job leases and ingest coalesce gates; see internal design docs `session-list-summary.md` and `async-jobs.md`.*
