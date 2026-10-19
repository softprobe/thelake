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
   Every automated test or evaluation that writes state must use transient,
   run-owned databases and workspaces. Never connect to or mutate a developer's
   running service, database, workspace, or other pre-existing state. Provision
   resources through shared fixtures/setup and tear them down automatically,
   including on failure. Stub external services when live behavior is not part
   of the test.
3. **Senior architect review loop.** For design and code changes, launch a
   senior-architect subagent that reads the relevant code, tests, documentation,
   and project goals. The review must be substantive, not a rubber stamp.
   Resolve every blocking finding and repeat review until green.
4. **Name by module and domain, not by plan.** Source files, modules, packages,
   tests, and identifiers must use code-module and business-domain phrases only.
   Never encode planning/roadmap vocabulary in the tree (`phase0`/`phase1`,
   leftovers, milestones, epics, sprints, “option A/B”). Specs and plans may use
   those words; implementation code must not.
5. **Document settings and major features with the implementation.** Update
   authoritative reference docs and relevant development skills in the same
   change. Add source comments where a setting's scope, lifecycle, or safety
   constraints are not obvious. Clearly label test-only settings and state
   which resources they create or modify.
6. **Verify the actual consumer.** When adding a skill, prompt, tool, hook, or
   configuration for an agent or product, trace how that runtime discovers and
   loads it. A developer skill under `.agents/skills/` does not become a product
   agent skill unless the product runtime loads or explicitly references it.
   Verify the user-facing agent instructions explain when to use newly exposed
   capabilities; do not claim an agent knows about a capability based only on
   its existence in an MCP server or developer skill.
7. **Use one Docker lifecycle for automated tests.** Tests must start and stop
   Docker resources through `scripts/test_stack.py`; Compose files only describe
   service topology. Never start test containers directly from fixtures or
   harness scripts, and never attach an automated test to a developer's stack.
   Keep reports and logs in `target/test-artifacts/`; the shared lifecycle
   removes run-owned containers, volumes, networks, and scratch data after the
   test. Operator workflows such as `make setup` and `make grafana-up` remain
   separate. See [`docs/how-to/test-environments.md`](docs/how-to/test-environments.md).
8. **Clean cutoffs.** Replace prior implementations and data formats directly;
   no migrations, dual paths, fallbacks, or compatibility unless the user
   explicitly asks. See
   [`docs/how-to/clean-cutoffs.md`](docs/how-to/clean-cutoffs.md).
9. **Actually run the relevant tests and evaluations.** Do not ship code
   changes without executing the covering unit, integration, e2e, and labeled
   evaluation suites; report what ran and the outcomes. See
   [`docs/how-to/change-validation.md`](docs/how-to/change-validation.md).
10. **Real agent execution validation.** Prompt, skill, tool, hook, routing,
    scheduling, or workflow changes require a real labeled evaluation with the
    configured model and runtime—not mocks or offline scorers. Check the
    repository-root `.env` (loaded via project dotenv) before claiming
    credentials unavailable. See
    [`docs/how-to/change-validation.md`](docs/how-to/change-validation.md).
11. **SQL safety.** No query SQL in code string literals; every `traces`/`logs`
    read is finitely time-bounded on bare `timestamp`; fact SQL only through
    checked paths; fix the harness instead of adding exemptions. See
    [`docs/how-to/sql-safety.md`](docs/how-to/sql-safety.md).

Prefer shared abstractions that remove duplication over parallel implementations.

# DuckLake catalog

Production uses a **Postgres** DuckLake catalog only. Do not reintroduce
sqlite/postgres catalog branching in production code. Workspace engines are
obtained only via `WorkspaceManager::workspace_for` (through `AppState`).
