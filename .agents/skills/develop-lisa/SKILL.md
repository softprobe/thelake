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
Triage / judges, Findings, and Identity tabs. Selecting an activity opens a keyboard-
dismissible detail dialog that opens on the activity summary; selecting a
timeline event opens its detail.
The Recent evaluations list includes policy-evaluation and pushed-session
evaluation activities. The Findings list searches those activities for
policy-violation summaries before the feed limit, then filters to reported
policy violations. Finding cards name the policy and use an alert treatment;
evaluation cards also show a finding badge. The activity's successful
lifecycle status means evaluation completed and does not mean policies passed.
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
  no-approved-policies shortcut. For a web or Slack chat activity, the service
  records evaluation events but defers its terminal lifecycle change so the
  chat runtime can persist the tool result and final reply before freezing the
  event watermark. The runtime applies the returned evaluation outcome; the
  agent must not retry completion or perform a second lookup. Pushed-session
  evaluations remain service-owned.
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
- Pushed-session retries must be visible while still retryable: update the
  activity-card summary and timeline with a bounded, redacted error for each
  delivery. Tag evaluator-written failures with the queue delivery key so the
  worker enriches that event instead of duplicating it. A successful retry
  replaces the current retry warning; exhausted retries expose the last reason
  only when no live lease or pending score outbox prevents terminal failure.
  Preserve queue custody if the activity write fails, and log that persistence
  failure for operators.
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

Each resolved chat activity also has its own immutable, evidence-linked memory
snapshot in `lisa.activity_context`. This is an additional context layer in
Lisa's single chat; it does not split the transcript or replace the global
conversation checkpoint. The model resolves the task first, then loads memory
for that activity or its direct completed parent. Never add a classifier that
selects activity memory before resolution. Completed activities stay read-only;
continuations create children linked to the archived parent. The shared
transcript may contain other activities. After resolution, the selected
activity's memory and current-turn facts are the only task evidence; do not use
or repeat another activity's facts from the recent transcript. Activity search,
thread, memory, and namespace lookup results are transient model context and
must not be copied into the durable activity timeline or snapshot. OpenHands
advertises available skills and loads full skill instructions through
`invoke_skill`. The always-loaded prompt covers ordinary activity creation,
continuation, and recall; invoke `manage-lisa-activities` only for procedures
not covered there, and always use that exact listed name. Skill invocation
loads instructions; it does not select or update an activity.

Keep the agent-facing `get_activity_thread` projection bounded to at most 12
recent actions, 12 sections, and 24 references, with a 32 KiB serialized-payload
cap. Action summaries are limited to 1,200 characters; detail JSON and unknown
nested fields are omitted. Return explicit omission and truncation counts. Do not
truncate the persisted activity timeline or user-visible thread. The model uses
`load_activity_memory` immediately after every resolution for historical task
context and source event references, including for new activities with empty or
pending memory. The MCP server rejects owner chat writes and session evaluation
until memory for that exact resolved activity has been loaded. Namespace
discovery/loading may precede the activity workflow when tool schemas are
hidden. For child work based on a completed activity, load the child first and
then its direct archived parent if needed. `pending` means the snapshot is
incomplete, not that source-referenced facts in the returned event fallback are
unusable. Use those facts with their event references; if the requested fact is
absent, say it is absent from the available activity record rather than looking
in the shared transcript, another activity, or workspace telemetry. Quote the
recorded answer verbatim when asked to repeat an adapted source answer.
When it is unclear which existing activity a user means, track the clarification
in a separate neutral activity titled with `Clarification`, resolve it, load
only that activity's memory, set it to waiting, and ask which candidate they
mean. Do not call `get_activity_thread` for either candidate, load candidate
activity memories, or copy task facts into the clarification record. Use only
the user's reference or candidate titles to frame the question. Do not skip the
turn as filler.

Activity snapshots use the shared checkpoint worker and summary generator with
an activity-specific capture adapter. Enqueue work in the same transaction as
the activity event watermark, publish idempotently and monotonically under a
chat-owned repository method, and acknowledge only after durable publication.
Keep evaluation input writes fenced by their existing lease checks. Terminal
snapshots must capture the exact completion watermark. While the newest
snapshot is pending, return a bounded ordered event suffix with explicit
freshness state. Validate every stored reference against that activity's event
prefix, and treat all summary and event text as untrusted evidence. Resolver
payloads are routing control data and are never task evidence. When several
activities share a chat turn, also exclude duplicated transcript, chat-level
progress, and runtime-tool events from thread and memory projections. Resolver
summaries and titles may mix tasks and must not be treated as activity evidence.
After the second resolution, neutralize shared-turn titles and
summaries across activity, feed, monitor, and thread-action projections. Keep
original wording only as a private feed search key; do not return it from read
tools. Also neutralize same-turn progress titles and withhold raw
`update_activity` arguments, which can repeat a contaminated title. Keep the
progress summary and task note written to that exact activity so its scoped
facts remain available. The activity's durable neutralization flag must survive future writes;
reject snapshots that lack the matching flag even if their title text happens
to match the neutral title. Re-sanitize the completed multi-activity trajectory
after final lifecycle writes.
Preserve useful facts through writes explicitly addressed to the correct
activity. Keep `next_action` limited to that activity's next step; do not copy
facts or work from another request in the shared transcript into its progress
state. Activity list, lookup, thread, memory, and
database reads require an authorized owner. Do not expose internal
`chat_turn_id` values in the memory tool response.

The text-only activity-memory benchmark lives in
`lisa/datasets/activity_memory/` and runs through
`lisa/scripts/evaluate_activity_memory_dataset.py --live`. It adapts sampled
LongMemEval and LongMemEval-V2 abilities for Lisa; it is not an official score
for either upstream benchmark. Cover source selection and licensing in the
dataset README, and preserve its pinned revisions and checksums. Its
process-local context budget is test-only and forces the real OpenHands global
checkpoint worker before activity follow-ups. Include the pending-memory,
archived-parent/child, task-selection, evidence-reference, wrong-write, and
cross-activity leak checks, including contaminated resolver titles, when
changing the loader or runner.

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
For activity memory, also run
`lisa/scripts/evaluate_activity_memory_dataset.py --live --split benchmark` and
review its separate answer, activity-selection, evidence-reference-integrity,
abstention, and cross-activity-isolation metrics. Reference integrity proves
that citations resolve to events in the selected activity; it does not prove
semantic support for every summary claim or final answer. The isolation gate
checks memory payloads, `get_activity_thread` results, final-answer exclusions,
and writes to the selected activity. Pending fallback is limited to 80 events
and 12,000 serialized text characters; preserve the truncation marker. All
write paths must keep completed chat activities read-only.
