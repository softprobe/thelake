# Engineering Principles

## Public Repository Disclosure

This repository is public. Never commit or push sensitive or internal information.
If content classification is unclear, keep it private and ask the user.

These requirements always apply to design, implementation, refactoring, and review:

1. **DRY first.** Reuse production and test code. Keep behavior in one shared
   implementation and isolate only the genuinely backend-specific primitives.
   Do not copy lifecycle orchestration, validation, fixtures, scenarios, or
   assertions between adapters.
2. **Senior architect review loop.** For design and code changes, launch a
   senior-architect subagent that reads the relevant code, tests, documentation,
   and project goals. The review must be substantive, not a rubber stamp.
   Resolve every blocking finding and repeat review until green.
3. **Name by module and domain, not by plan.** Source files, modules, packages,
   tests, and identifiers must use code-module and business-domain phrases only.
   Never encode planning/roadmap vocabulary in the tree (`phase0`/`phase1`,
   leftovers, milestones, epics, sprints, “option A/B”). Specs and plans may use
   those words; implementation code must not.
4. **Document settings and major features with the implementation.** Update
   authoritative reference docs and relevant development skills in the same
   change. Add source comments where a setting's scope, lifecycle, or safety
   constraints are not obvious. Clearly label test-only settings and state
   which resources they create or modify.
5. **Verify the actual consumer.** When adding a skill, prompt, tool, hook, or
   configuration for an agent or product, trace how that runtime discovers and
   loads it. A developer skill under `.agents/skills/` does not become a product
   agent skill unless the product runtime loads or explicitly references it.
   Verify the user-facing agent instructions explain when to use newly exposed
   capabilities; do not claim an agent knows about a capability based only on
   its existence in an MCP server or developer skill.
6. **No toxic leftovers.** The tree is for a reader with no context of how you
   got here—they care about the business, not your mistakes. Never leave dead
   paths, shims, orphaned docs, dual implementations, or other archaeology.
   When you add or replace something, delete what it supersedes in the same
   change. See [`docs/how-to/clean-cutoffs.md`](docs/how-to/clean-cutoffs.md).
7. **Clean cutoffs.** Never add backward compatibility or migrations. Assume
   the prior implementation and its data formats never existed; replace schema
   and contracts directly and update callers, docs, and tests together.
   Compatibility only when the user explicitly asks. See
   [`docs/how-to/clean-cutoffs.md`](docs/how-to/clean-cutoffs.md).
8. **SQL safety.** No query SQL in code string literals; every `traces`/`logs`
   read is finitely time-bounded on bare `timestamp`; fact SQL only through
   checked paths; fix the harness instead of adding exemptions. See
   [`docs/architecture/sql-and-schema.md`](docs/architecture/sql-and-schema.md).
9. **TDD and complete coverage.** Write a failing test before implementation.
   Every change requires proportionate unit, integration, and end-to-end
   coverage. Prefer one shared contract suite over copying tests. Every
   automated test or evaluation that writes state must use transient, run-owned
   databases and workspaces—never a developer's running stack. See
   [`docs/how-to/test-and-eval.md`](docs/how-to/test-and-eval.md).
10. **Use one Docker lifecycle for automated tests.** Start and stop Docker
    through `scripts/test_stack.py`; keep reports under `target/test-artifacts/`.
    Never attach an automated test to a developer's stack. See
    [`docs/how-to/test-and-eval.md`](docs/how-to/test-and-eval.md).
11. **Actually run the relevant tests and evaluations.** Do not ship code
    changes without executing the covering unit, integration, e2e, and labeled
    evaluation suites; report what ran and the outcomes. See
    [`docs/how-to/test-and-eval.md`](docs/how-to/test-and-eval.md).
12. **Real agent execution validation.** Prompt, skill, tool, hook, routing,
    scheduling, or workflow changes require a real labeled evaluation with the
    configured model and runtime—not mocks or offline scorers. Check the
    repository-root `.env` (loaded via project dotenv) before claiming
    credentials unavailable. See
    [`docs/how-to/test-and-eval.md`](docs/how-to/test-and-eval.md).

Prefer shared abstractions that remove duplication over parallel implementations.

# DuckLake catalog

Production uses a **Postgres** DuckLake catalog only. Do not reintroduce
sqlite/postgres catalog branching in production code. Workspace engines are
obtained only via `WorkspaceManager::workspace_for` (through `AppState`).
