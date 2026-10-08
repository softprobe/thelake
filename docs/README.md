# Documentation

Pick the page by what you are trying to do.

| Goal | Start here |
|------|------------|
| Run, build, CI, Docker | Root [`README.md`](../README.md) |
| Understand how the system works | [Architecture](architecture/overview.md) |
| Complete a task | [How-to guides](#how-to) |
| Look up an API, config key, or compat route | [Reference](#reference) |
| Softprobe SDK attribute / observation contracts | [SDK contracts](sdk/README.md) |
| Agent engineering invariants | [`AGENTS.md`](../AGENTS.md) |

## Architecture

Concepts and runtime design (read for understanding, not step-by-step).

- [Overview](architecture/overview.md) — ingest, DuckLake storage, query, maintenance
- [Product goals](architecture/goals.md)
- [Workspace identity](architecture/workspace-identity.md) — `workspace_id` and physical scopes
- [SQL and schema](architecture/sql-and-schema.md) — tables, one-clock rules
- [Event-time layout](architecture/event-time-layout.md) — partition pruning
- [Attribute storage](architecture/attribute-storage.md) — `MAP` columns and promotion
- [DuckLake access inventory](architecture/ducklake-access.md) — engine ownership boundaries

## How-to

- [5-minute quickstart](quickstart.md) — create a check in chat, run a Gemini sample agent, and inspect the result
- [Instrument applications](how-to/instrumentation.md)
- [Use Explorer](how-to/explorer.md) — browser chat and session/trace UI at `/explorer/`
- [Use Slack behavior checks](how-to/slack-evaluator.md) — author checks and receive online evaluation failures in threads
- [Apply schema promotion](how-to/promotion.md)
- [Query DuckLake locally](how-to/adhoc-duckdb.md)
- [Operate async jobs](how-to/async-jobs.md)
- [Session list summaries](how-to/session-summaries.md)

## Reference

- [HTTP OpenAPI](reference/openapi.yaml) — served as `GET /openapi.json`
- [Configuration](reference/config.md) — YAML sections, defaults, env overrides
- [Loki / Tempo compatibility](compat/README.md)
- [Performance gates](perf/README.md)
- Promotion manifests (applied at runtime): [`promotion/`](promotion/)
- Event-time EXPLAIN fixtures: [`fixtures/`](fixtures/)

## SDK contracts

Language-neutral Softprobe SDK docs live under [`sdk/`](sdk/README.md).
Machine-readable schemas are under [`contracts/`](../contracts/README.md).
The runtime does not implement those SDK packages; it stores the OTLP they emit.

## Source of truth

| Fact | Authority |
|------|-----------|
| HTTP routes + JSON shapes | [`reference/openapi.yaml`](reference/openapi.yaml) and `src/api/` |
| Config keys and defaults | [`reference/config.md`](reference/config.md) and `src/config.rs` |
| Table DDL | `src/sql/schema/*.sql` |
| SQL safety invariants | [`AGENTS.md`](../AGENTS.md) |
| Make targets | `Makefile` / [`scripts/README.md`](../scripts/README.md) |
