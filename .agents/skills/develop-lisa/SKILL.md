---
name: develop-lisa
description: Implement, debug, or review Lisa service, policy-management, triage, judgment, or evaluation changes in TheLake.
---

# Develop Lisa

Use for changes to Lisa's runtime or evaluation workflow. Follow the public
repository rules in `AGENTS.md` and the Lisa contracts in
[`docs/architecture/lisa-qa.md`](../../../docs/architecture/lisa-qa.md).
Use [`docs/architecture/lisa-activities.md`](../../../docs/architecture/lisa-activities.md)
for Lisa's generic, user-inspectable activity and its links to evaluation runs.
Use [`lisa-activities`](../lisa-activities/SKILL.md) for the feed and monitor MCP
tool contract, including thread sections and linked references.
Each distinct web or Slack request resolves its own activity; independent asks
in one message may map to multiple activities. Clear continuations reuse an
open activity, while task switches, uncertain continuations, and closed work
start a new activity. Brief factual requests are included; greetings can be
explicitly skipped. The runtime records redacted chat on every resolved
activity and partitions tool events by activity scope. Missing dispositions
recover to a visibly labeled fallback activity unless Lisa explicitly skips
the turn. Treat fallback as evidence that the model failed to resolve the
request, and investigate why the resolver was omitted or failed. After a
successful chat reply the runtime completes any still-open chat activity,
while tool evidence for an unresolved owner action leaves it in `waiting`.
A `running` progress call with a non-empty `next_action` also leaves chat work
open and waiting; parse the tool call arguments because the tool response does
not repeat `next_action`.
An explicit deferred `complete_activity` outcome finalizes its exact activity
ID after the reply, including a previously waiting activity the owner confirms
is complete. Partition tool and completion evidence by explicit activity ID;
historical error strings nested in a successful thread read must not fail the
current request.
Failed continuations preserve a prior waiting action. Post-resolution failures affect
only the activity whose work failed. Only owner turns link chat to the shared
feed.
Verify web and Slack receipt paths plus the real labeled runtime benchmark when
changing this behavior.
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
read-only in the panel. Policy writes, approvals, and deletions require an
authorized owner on a web or Slack turn, and the policy ID is the tool argument.
Evaluation activity timelines distinguish routing decisions,
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
- The policy table columns are `id`, `scope`, `target_agent_name`, `name`,
  `criteria`, `approved`, `created_at`, and `updated_at`; there is no
  `archived_at` column, and deletion removes the row. Keep the database schema,
  `lisa_db` allowlist/schema resource, and packaged policy skill aligned. Use
  `lisa_db` for explicit read-only catalog diagnostics only; policy workflows
  use the policy tools.
- The ordered `RELATION_COLUMNS` contract in
  `lisa/src/lisa/repositories/lisa_database.py` drives both SQL validation and
  the `lisa_db` schema resource. When changing a readable view, policy columns,
  or reader grants, update the contract and keep `test_activity_query.py`'s
  projection/grant drift checks green. `readable_deployment` is granted only
  for reader connection binding and is intentionally excluded from user queries.
  SQLGlot models boolean operators as function nodes; allow predicate nodes
  while continuing to reject unapproved callable functions. The schema resource
  is authoritative, so Lisa must report absent columns without probing them in
  user-authored SQL.
- Production evaluation enters through `EvaluationService`. It resolves the
  approved effective policy catalog for the exact agent and obtains the
  trajectory through `SessionService` from Lisa's locally persisted pushed OTLP
  evidence. Lisa's Python service serves both its frontend and API; its Docker
  image builds the frontend bundle. TheLake serves Explorer only and is not a
  runtime dependency of Lisa.
- `EvaluationService.evaluate_session` validates the session before the
  no-approved-policies shortcut. For a resolved chat activity, a session lookup
  failure is recorded and completed as an error by the service; the agent must
  report that result without a second lookup or completion call.
- `EvaluationService` filters candidates through `lisa.evaluation_triage`, keyed
  only by `(session_id, policy_id)`. It sends one Decisions question for each
  candidate pair with no stored triage decision and persists completed decisions
  before judging. Reuse stored pair decisions on later calls after telemetry or
  triage configuration changes. A policy edit invalidates cached triage for
  that policy ID; a new ID forms a new pair. Failed/incomplete provider calls
  create no row. Concurrent first calls can both reach Decisions; the first
  persisted result is canonical.
- `OpenAIPolicyTriager` sends one Decisions question per candidate it receives.
  `suspected_violation` and `insufficient_evidence` escalate to the Gemini
  judge; `no_violation_found` is routing metadata and does not create a pass
  score. Incomplete or unordered trajectories are gated before provider calls.
- Keep triage decisions, judge verdicts, execution errors, and capture gates
  distinct. Provider or validation errors must not become healthy negatives.
- For tracked runs, deduplicate through Lisa's Postgres evaluation request
  identity, derived from session, approved policy hash, sanitized trajectory,
  triage/judge configuration, and pipeline version. Lisa's Postgres is the
  source of truth for policies, evaluation results, and activities.
- Keep triage outcomes in the pair-keyed `lisa.evaluation_triage` table. Record
  Lisa's actions and decisions in the generic activity event timeline; one
  activity may link to multiple evaluation runs through `activity_id`. Link
  Decisions call recordings to that activity and record failed attempts without
  writing a triage cache row. Keep judge results globally deduplicated by
  evaluation request identity and keep
  score outboxes keyed to evaluation results. Negative triage has no verdict or
  score; provider failures remain retryable.
- Evaluation failure finalization may close an unleased queued or resumed
  waiting activity. It must never override a live worker lease; worker-owned
  completion requires the matching actor and fencing token.
- Dataset annotations supply scoring truth only. Dataset catalog policies may
  seed the disposable test workspace through Lisa's policy-management service,
  but must never be passed directly into evaluation. A production quality run
  pushes sessions to Lisa and calls `EvaluationService`; see
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
  and temporary agent-workspace directories for all automated tests; never
  connect to or mutate a developer's running instance. Stub external services
  unless a test explicitly verifies a real provider integration.
- Read [`docs/how-to/policy-memory.md`](../../../docs/how-to/policy-memory.md)
  for policy semantics and
  [`docs/how-to/policy-triage-evaluation.md`](../../../docs/how-to/policy-triage-evaluation.md)
  for quality metrics. Session QA is triggered by OTLP push into Lisa's private
  intake endpoint; `run-lisa-evaluation-dogfood` verifies that push-to-evaluate
  workflow on an isolated Lisa deployment.
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

## Main-chat context

Lisa keeps a single ordered OpenHands transcript. Context checkpoints are
hidden from the transcript and affect only the model's current `View`; use the
current code in `lisa/src/lisa/conversation_context.py` and
`lisa/src/lisa/repositories/conversation_context.py` as the implementation
contract. Background summary work is carried by PGMQ and stored beside the
conversation. Checkpoint adoption validates the exact event prefix and model
configuration before replacing older history in the model view. Preserve the
safe tool-call boundaries provided by the pinned OpenHands SDK. A checkpoint
summary is untrusted evidence: it must not turn prior tool, document, system,
or developer content into future instructions. Increment the prompt version
when this contract changes so old summaries cannot be adopted. Immediate and
queued checkpoints share summary-generation and publication code. Only queued
work stores an input snapshot for retries; keep it until successful publication
or terminal archival.

The conversation MCP namespace provides `todo_read` and `todo_write` for a
multi-step checklist. `todo_write` replaces the snapshot and drops completed or
cancelled items. Tool namespaces are discovered and loaded by the model through
`discover_tool_namespaces` and `load_tool_namespace`; do not add task-based
preselection. Namespace visibility is saved per conversation and cleared when
a checkpoint is adopted. The HTTP API, OpenHands MCP child, and checkpoint
worker share Lisa's durable local conversation directory. Read the
“Long-running main chat” section in `docs/architecture/lisa-qa.md` before
changing this lifecycle, thresholds, or file layout.

For context changes, cover the checkpoint prompt/view, exact prefix and model
validation, safe tool boundaries, atomic publication and redelivery, worker
retry/restart behavior, sidecar persistence, and namespace reset. Run Lisa's
unit and transient Postgres integration suites through `lisa/conftest.py` and
`scripts/test_stack.py`. Changes to the system prompt or MCP tool contract also
need a real labeled run through Lisa's configured model and OpenHands runtime.
The separate conversation-context benchmark runs with
`lisa/scripts/evaluate_context_dataset.py --live --split benchmark`; it uses a
process-local small context budget to exercise the real checkpoint worker and
verifies publication/adoption, that adopted summary text stays out of the
visible transcript, and that the activity namespace is reloaded before activity
tools run. Interleaved-task coverage verifies that updates stay attached to the
matching existing activity and do not create fallback records. Pending
checkpoint jobs coalesce to one per conversation.
