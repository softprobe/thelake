---
name: develop-lisa
description: Implement, debug, or review Lisa service, policy-management, triage, judgment, scheduling, or evaluation changes in TheLake.
---

# Develop Lisa

Use for changes to Lisa's runtime or evaluation workflow. Follow the public
repository rules in `AGENTS.md` and the Lisa contracts in
[`docs/architecture/lisa-qa.md`](../../../docs/architecture/lisa-qa.md).

## Source of truth and flow

- Use [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md):
  `session_id` identifies the evaluation input, trajectory is its derived
  evidence projection with no separate ID, and policy pairs use `policy_id`.
- Policies are owned by Lisa's PostgreSQL `lisa.policies` catalog. Use
  `PolicyService` and its repository for policy operations; never make a
  quality evaluation by passing annotation-selected policies directly to the
  evaluator.
- Production evaluation enters through `EvaluationService`. It resolves the
  approved effective policy catalog for the exact agent and obtains the
  trajectory through `SessionService` / TheLake's normal session path.
- `OpenAIPolicyTriager` sends one Decisions question per approved candidate.
  `suspected_violation` and `insufficient_evidence` escalate to the Gemini
  judge; `no_violation_found` is routing metadata and does not create a pass
  score. Incomplete or unordered trajectories are gated before provider calls.
- Keep triage decisions, judge verdicts, execution errors, and capture gates
  distinct. Provider or validation errors must not become healthy negatives.
- For tracked runs, deduplicate through Lisa's Postgres evaluation request
  identity, derived from session, approved policy hash, sanitized trajectory,
  triage/judge configuration, and pipeline version. TheLake scores are output
  copies, not the source of truth for whether Lisa already evaluated evidence.
- Persist each candidate's triage outcome in the canonical evaluation ledger
  before judging. Negative triage is a completed `not_run` judge outcome with no
  verdict or score; provider failures remain retryable. Do not add a second
  identity or evaluation path for triage.
- Dataset annotations supply scoring truth only. Dataset catalog policies may
  seed the disposable test workspace through Lisa's policy-management service,
  but must never be passed directly into evaluation. A production quality run
  ingests sessions into TheLake and calls `EvaluationService`; see
  [`run-lisa-policy-evaluation`](../run-lisa-policy-evaluation/SKILL.md).

## Work and verification

- Lisa tests follow the repository-wide transient-resource rule in `AGENTS.md`.
  They use a disposable Postgres database per test through
  [`lisa/tests/conftest.py`](../../../lisa/tests/conftest.py). It starts the
  `lisa-postgres` service in the shared transient test Compose stack and creates
  an isolated database and writer/reader roles for each test. Removing the
  container at session teardown removes all test databases and roles; no
  external database URL is supported. Use transient Lisa databases and
  TheLake workspaces for all automated tests; never connect to or mutate a
  developer's running instance. Use these fixtures for database-backed
  behavior, and stub external service calls when the test does not need a real
  TheLake server.
- Read [`docs/how-to/policy-memory.md`](../../../docs/how-to/policy-memory.md)
  for policy semantics and
  [`docs/how-to/policy-triage-evaluation.md`](../../../docs/how-to/policy-triage-evaluation.md)
  for quality metrics. Use `run-lisa-evaluation-dogfood` for the scheduled
  conversation-to-score workflow.
- Make the smallest change that preserves the production service path. Keep
  provider adapters in `lisa/evaluation/`, orchestration in
  `lisa/services/evaluations.py`, and persistence behind Lisa repositories.
- For behavior changes, add tests at the affected service boundary and the
  relevant provider or API contract. Run the focused Lisa tests; use the
  repository's `run-thelake-tests` skill when TheLake integration is affected.
- Update Lisa architecture/how-to docs and the relevant `.agents/skills/` when
  policy storage, triage outcomes, escalation, score semantics, or evaluation
  setup changes. Treat code as authoritative and do not describe harness-only
  diagnostics as production quality evidence.
- Review the diff for public-repository disclosure, accidental dataset or
  credential changes, and claims that no longer match runtime behavior.
