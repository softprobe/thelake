---
name: develop-lisa
description: Implement, debug, or review Lisa service, policy-management, triage, judgment, scheduling, or evaluation changes in TheLake.
---

# Develop Lisa

Use for changes to Lisa's runtime or evaluation workflow. Follow the public
repository rules in `AGENTS.md` and the Lisa contracts in
[`docs/architecture/lisa-qa.md`](../../../docs/architecture/lisa-qa.md).
Use [`docs/architecture/lisa-activities.md`](../../../docs/architecture/lisa-activities.md)
for Lisa's generic, user-inspectable activity and its links to evaluation runs.
Use [`lisa-activities`](../lisa-activities/SKILL.md) for the feed and monitor MCP
tool contract, including thread sections and linked references.
Substantive web and Slack chat turns must select or create one activity; the
runtime records redacted messages and tool calls/results in sequence, links
their source IDs, and refreshes the Lisa feed and open detail view. Attaching
chat content to shared activities requires a web or Slack turn from an actor in
`LISA_OWNER_USER_IDS`. Verify the web and actual Slack receipt paths plus the
real runtime benchmark when changing this behavior.
Lisa's packaged runtime skills live in [`lisa/skills/`](../../../lisa/skills/)
and are copied into the Lisa image by `lisa/Dockerfile`; changes to those skills
must be verified through `OpenHandsBackend`'s configured skill directory.

The Explorer Lisa view keeps chat visible beside Activities, Rules,
Triage / judges, and Identity tabs. Selecting an activity opens a keyboard-
dismissible detail dialog that opens on the activity summary; selecting a
timeline event opens its detail.
Keep the shell aligned to Muse's visible layout: 72px navigation rail, 360px
right panel, and centered 960×700px activity dialog at the 1610×1505 desktop
reference viewport. Reuse the profile, segmented tabs, activity rows, modal,
timeline, and typography treatment; put Lisa-specific data inside those slots.
Check the rendered page against Muse at the same viewport and device scale.
Browser routes must reuse the authenticated
workspace binding and expose only bounded, purpose-built projections. Rules are
read-only in the panel; policy changes still use the exact-owner-confirmation
approval flow. Evaluation activity timelines distinguish routing decisions,
judge verdicts, and failures. The current runtime identity is configured by
`LISA_SYSTEM_PROMPT` and packaged skills; do not present `SOUL.md` or
`MEMORY.md` as existing editable files until Lisa has a canonical source and
authorization contract for them.

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
- `EvaluationService` filters candidates through `lisa.evaluation_triage`, keyed
  only by `(session_id, policy_id)`. It sends one Decisions question for each
  candidate pair with no stored triage decision and persists completed decisions
  before judging. Reuse stored pair decisions on later calls even after
  telemetry, policy content, or triage configuration changes; new IDs form new
  pairs. Failed/incomplete provider calls create no row. Concurrent first calls
  can both reach Decisions; the first persisted result is canonical.
- `OpenAIPolicyTriager` sends one Decisions question per candidate it receives.
  `suspected_violation` and `insufficient_evidence` escalate to the Gemini
  judge; `no_violation_found` is routing metadata and does not create a pass
  score. Incomplete or unordered trajectories are gated before provider calls.
- Keep triage decisions, judge verdicts, execution errors, and capture gates
  distinct. Provider or validation errors must not become healthy negatives.
- For tracked runs, deduplicate through Lisa's Postgres evaluation request
  identity, derived from session, approved policy hash, sanitized trajectory,
  triage/judge configuration, and pipeline version. TheLake scores are output
  copies, not the source of truth for whether Lisa already evaluated evidence.
- Keep triage outcomes in the pair-keyed `lisa.evaluation_triage` table. Record
  Lisa's actions and decisions in the generic activity event timeline; one
  activity may link to multiple evaluation runs through `activity_id`. Link
  Decisions call recordings to that activity and record failed attempts without
  writing a triage cache row. Keep judge results globally deduplicated by
  evaluation request identity and keep
  score outboxes keyed to evaluation results. Negative triage has no verdict or
  score; provider failures remain retryable.
- Dataset annotations supply scoring truth only. Dataset catalog policies may
  seed the disposable test workspace through Lisa's policy-management service,
  but must never be passed directly into evaluation. A production quality run
  ingests sessions into TheLake and calls `EvaluationService`; see
  [`run-lisa-policy-evaluation`](../run-lisa-policy-evaluation/SKILL.md).

## Activity and verification

- Lisa tests and live evaluation harnesses follow the repository-wide
  transient-resource rule in `AGENTS.md`. **All** disposable environment
  setup/teardown lives in [`lisa/conftest.py`](../../../lisa/conftest.py)
  (`start_lisa_postgres` / `stop_lisa_postgres`, `create_lisa_test_database`,
  `provision_lisa_schema`, `configure_lisa_dogfood` /
  `dispose_lisa_dogfood`). Pytest fixtures re-export those helpers;
  `lisa/benchmark/` runners must import them and must not own Compose,
  database, role, schema, or dogfood lifecycle. One Postgres container may
  be reused for a whole pytest session or benchmark run, but every tracked
  stack must be stopped when the session/process ends (fixture teardown,
  `pytest_sessionfinish`, and `atexit`) so interrupted runs do not leave
  orphan containers. Removing the container removes all test databases and
  roles; no external database URL is supported. Use transient Lisa databases
  and TheLake workspaces for all automated tests; never connect to or mutate a
  developer's running instance. Stub external service calls when the test
  does not need a real TheLake server.
- Read [`docs/how-to/policy-memory.md`](../../../docs/how-to/policy-memory.md)
  for policy semantics and
  [`docs/how-to/policy-triage-evaluation.md`](../../../docs/how-to/policy-triage-evaluation.md)
  for quality metrics. Use `run-lisa-evaluation-dogfood` for the scheduled
  conversation-to-score workflow.
- Make the smallest change that preserves the production service path. Keep
  provider adapters in `lisa/src/lisa/evaluation/` (trajectory, triage, judge,
  scores only), orchestration in `lisa/services/evaluations.py`, and persistence
  behind Lisa repositories.   Java-style layout: `src/lisa` (main), `tests` (all UTs), `datasets` (labeled
  resources + loaders), `benchmark` (live runners only). Runtime code must not
  import `benchmark` or `datasets`; tests may import them to unit-test
  loaders/scorers/runners.
- For behavior changes, add tests at the affected service boundary and the
  relevant provider or API contract. Run the focused Lisa tests; use the
  repository's `run-thelake-tests` skill when TheLake integration is affected.
- Update Lisa architecture/how-to docs and the relevant `.agents/skills/` when
  policy storage, triage outcomes, escalation, score semantics, or evaluation
  setup changes. Treat code as authoritative; the only labeled policy quality
  path is `EvaluationService` via `evaluate_policy_dataset.py`.
- Review the diff for public-repository disclosure, accidental dataset or
  credential changes, and claims that no longer match runtime behavior.
