# Engineering Principles

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

# DuckLake catalog

Production uses a **Postgres** DuckLake catalog only. Do not reintroduce
sqlite/postgres catalog branching in production code. Workspace engines are
obtained only via `RuntimeEngineManager::engine_for` (through `AppState`).
