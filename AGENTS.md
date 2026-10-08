# Engineering Principles

## Public Repository Disclosure

This repository is public. Never commit or push sensitive or internal information.
If content classification is unclear, keep it private and ask the user.

These requirements always apply to design, implementation, refactoring, and review:

1. **DRY first.** Reuse production and test code. Keep behavior in one shared
   implementation and isolate only the genuinely backend-specific primitives.
   Do not copy lifecycle orchestration, validation, fixtures, scenarios, or
   assertions between adapters.
2. **TDD and complete coverage.** Write a failing test before implementation.
   Every change requires proportionate unit, integration, and end-to-end
   coverage. Prefer one shared contract suite over copying tests.
3. **Senior architect review loop.** For design and code changes, launch a
   senior-architect subagent that reads the relevant code, tests, documentation,
   and project goals. The review must be substantive, not a rubber stamp.
   Resolve every blocking finding and repeat review until green.
4. **Name by module and domain, not by plan.** Source files, modules, packages,
   tests, and identifiers must use code-module and business-domain phrases only.
   Never encode planning/roadmap vocabulary in the tree (`phase0`/`phase1`,
   leftovers, milestones, epics, sprints, “option A/B”). Specs and plans may use
   those words; implementation code must not.

Prefer shared abstractions that remove duplication over parallel implementations.
Especially enforce rules 1, 2, and 4.

# SQL safety invariants

These are hard repository contracts, not review preferences:

1. **No query SQL in code string literals.** Put query statements in `.sql` files and load/render them from code. Small non-query fragments (identifiers, predicates, projection fragments) may be composed by typed/query-builder helpers, but must not contain complete SQL statements.
2. **Every `traces` or `logs` read is finitely time-bounded.** It must carry both a lower and an upper bare-`timestamp` predicate so DuckLake can partition-prune. Never add an unbounded historical scan, including lookup/detail queries that already have trace/session IDs.
3. **Do not bypass the execution gate.** Fact-table SQL must execute through the checked query/engine paths. Do not add raw DuckDB execution sites to work around a rejected query.
4. **Fix the harness instead of adding exceptions.** If a legitimate query is rejected, preserve the invariant and improve the template/builder/gate. Do not add a file, directory, or call-site exemption merely to make CI green.

`scripts/check_sql_guardrails.py` blocks newly introduced inline SQL and checks changed `.sql` templates that read traces/logs. Runtime DuckDB plan checks remain the final enforcement layer for time bounds.

# DuckLake catalog

Production uses a **Postgres** DuckLake catalog only. Do not reintroduce
sqlite/postgres catalog branching in production code. Workspace engines are
obtained only via `WorkspaceManager::workspace_for` (through `AppState`).
