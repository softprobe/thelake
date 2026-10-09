---
name: develop-thelake
description: Implement, debug, or review code in the public TheLake repository using its architecture, safety invariants, and test workflows.
---

# Develop TheLake

Use this for code changes in this repository. Keep the change inside the public
open-source product; never add private cloud implementation details, credentials,
customer data, or internal-only endpoints.

## Before editing

- Read the repository `AGENTS.md` and the relevant authoritative docs. Route by
  task through `docs/README.md`.
- For structural questions, use the configured CodeGraph MCP (`context`, then
  a focused `explore`). If it is not initialized, ask before initializing it.
- Check `git status` and preserve existing user changes. Do not switch branches
  or discard work without a clear reason.

## Implementation constraints

- Follow the public-repository disclosure rules and the engineering / SQL
  invariants in `AGENTS.md`.
- Write a failing test before implementation and add proportionate unit,
  integration, and end-to-end coverage for behavior changes. Prefer shared
  contract tests over duplicated backend-specific suites.
- For design and code changes, launch a senior-architect subagent to review the
  relevant code, tests, docs, and product goals. Resolve blocking findings and
  repeat the review until the review is green.
- Keep workspace engine access behind `WorkspaceManager::workspace_for` via
  `AppState`. DuckLake catalog production is PostgreSQL-only.
- Keep source names focused on code modules and business domains, not plans or
  milestones.

## Finish

Run the most relevant checks from `run-thelake-tests`. Review the diff for
public disclosure, SQL guardrails, generated artifacts, and unintended changes.
Report behavior changed, tests run, tests not run, and remaining limitations.
