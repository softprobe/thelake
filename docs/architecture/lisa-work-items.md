# Lisa work items and run context

> This document describes the durable work-item architecture and its intended
> boundaries. For the current policy-evaluation request path, see
> [Lisa QA](lisa-qa.md): `EvaluationService` loads approved candidate policies
> from PostgreSQL, calls Decisions triage for every candidate, and sends only
> suspected violations or insufficient evidence to the Gemini judge. A triage
> negative is not a passing score.
>
> Use [Lisa evaluation terms](lisa-terms.md) for the meaning of `session_id`,
> trajectory, `policy_id`, and the session-policy evaluation pair.

## Purpose

Lisa should remain one assistant with one main conversation. That conversation is
the communication channel, not the record of every job she undertakes. An
evaluation, scheduled evaluation batch, or proactive task is a separate work
item with its own lifecycle, event history, and context. The chat can announce
or discuss a work item, while the work item's durable state lives in Postgres.

This design borrows two useful patterns from Muse:

- `muse.db` exposes a documented schema to the agent for bounded, read-only
  investigation. Ordinary state changes still go through purpose-built tools.
- A long-running activity has its own tracked identity and working context,
  separate from the continuing chat. The chat can report progress without
  becoming the activity's storage system.

The Lisa implementation should adapt those boundaries, not copy Muse's schema
or product-specific activity types.

## Decisions

1. **Postgres owns Lisa work-item state.** Lisa's run status, lifecycle,
   approvals, events, context index, and result references are stored in a
   Lisa-owned operational schema. Creating or updating these records does not
   call TheLake's Rust service APIs.
2. **TheLake remains the telemetry and score system.** Lisa reads session detail
   through the existing TheLake query path and writes the final evaluation score
   through the existing score path. A Lisa work item stores references to that
   trajectory and score. It does not duplicate the telemetry warehouse.
3. **The main conversation is not run context.** A work item gets a dedicated
   context directory and a bounded set of context records. Lisa must not load
   the main conversation transcript as implicit context for a run.
4. **The filesystem and database have distinct jobs.** Postgres is canonical
   for identity, status, relationships, approvals, ordered events, and
   checkpoints. The run directory holds larger working files and generated
   artifacts. Rows point to files by relative path and content hash; files do
   not silently override database status.
5. **Lisa can inspect the schema and query it directly through a restricted
   read-only database tool.** A dedicated reader role can select approved views
   and the policy table; a separate writer role backs typed lifecycle operations. Neither
   runtime role owns the schema or can run DDL. State changes use typed
   operations and transactions, not model-authored `INSERT`, `UPDATE`, or
   `DELETE` statements.
6. **Postgres owns Lisa policy records.** Each policy has a stable string ID,
   workspace or exact-agent scope, name, criteria, approval state, and timestamps.
   Typed policy tools preserve owner confirmation; the read-only SQL tool may
   inspect `lisa.policies` but cannot change policy text or approval. At judge
   time, Lisa loads the current approved rule by policy ID and passes it to the
   evaluator; evaluation rows keep the policy ID, not a copied policy body.
   In the implemented policy-evaluation path, the service resolves the effective
   approved candidate set, sends every candidate to Decisions, retains each
   routing decision, and supplies only escalated policies to the judge.
7. **Consequential actions require explicit approval.** Approval is attached
   to the exact work item and proposed action, and is recorded as a durable
   decision by a trusted user-facing transport. The model cannot approve its
   own request. A general chat message or old approval must not authorize a
   different action.

One Lisa deployment is bound to one workspace at a time, so the Lisa tables do
not repeat a `workspace_id` column. Bind the Lisa database/schema and credentials
to that deployment's configured workspace; do not share one Lisa schema across
workspaces. The operational schema may share a Postgres server with the
product, but it must be a separate Lisa-owned database or schema and role. It
must not add Lisa tables to the DuckLake catalog or bypass `WorkspaceManager`
for access to workspace analytics. Provision it as an explicit Lisa dependency
with its own schema provisioning and backup/retention policy. If Lisa later serves
multiple workspaces in one database, add tenant keys and database-enforced row
scoping before enabling that deployment model.

## Data model

Use a small normalized model. `work_items` gives all evaluations and
proactive jobs one lifecycle. `evaluation_runs` holds evaluation-specific
inputs and result references. Events form the activity timeline. Context items
are the durable, inspectable working set, while files hold larger artifacts.
An evaluation can be a work item by itself or a child of a scheduled batch.
Subtasks can use the same parent/child relation if parallel execution is
introduced.

| Table | Responsibility | Important fields |
|---|---|---|
| `lisa.work_items` | Canonical activity and execution state | `id`, `kind`, `parent_id`, `status`, `title`, `summary`, `source`, `created_at`, `started_at`, `updated_at`, `finished_at`, `context_path`, `lease_owner`, `lease_expires_at`, `fencing_token`, `attempt`, `version` |
| `lisa.evaluation_runs` | Evaluation-specific inputs/results | `work_item_id`, `policy_id`, `target_agent_name`, `session_id`, `policy_name`, `policy_hash`, `judge_config`, `score_id`, `verdict`, `started_at`, `finished_at` |
| `lisa.work_events` | Append-only ordered activity history | `work_item_id`, `seq`, `event_type`, `title`, `summary`, `actor`, `payload_ref`, `created_at` |
| `lisa.work_context` | Curated context, references, and checkpoints | `id`, `work_item_id`, `seq`, `kind`, `content`, `source_ref`, `content_hash`, `created_at` |
| `lisa.approval_requests` | Explicit approval boundary for a proposed action | `id`, `work_item_id`, `action`, `action_hash`, `scope`, `status`, `requested_at`, `expires_at`, `resolved_at`, `resolved_by`, `decision_ref`, `consumed_at` |
| `lisa.score_outbox` | Durable intent and retry state for score writes | `id`, `work_item_id`, `idempotency_key`, `score_payload`, `payload_hash`, `status`, `attempt`, `score_id`, `created_at`, `acknowledged_at` |
| `lisa.policies` | Current workspace and agent policy rules | `id` (text), `scope`, `target_agent_name`, `name`, `criteria`, `approved`, `created_at`, `updated_at`, `archived_at` |

Evaluation verdicts use Lisa's existing outcome vocabulary: `pass`, `fail`,
`uncertain`, and `insufficient_evidence`.

`work_items.kind` starts with `evaluation`, `evaluation_batch`, and
`proactive_task`. `status` uses a constrained set: `queued`, `running`,
`waiting_approval`, `waiting`, `succeeded`, `failed`, and `cancelled`.
Terminal status is derived from a committed transition; it is not inferred
from the chat transcript. Every transition appends an event in the same
transaction.

Useful database constraints include:

- a foreign key from every child row to its parent work item, including a check
  that `parent_id` points to an item owned by this Lisa deployment;
- a unique `(work_item_id, seq)` for ordered events and context items;
- a uniqueness key for an evaluation request, such as
  `(policy_hash, session_id, request_key)`, so retries do not accidentally
  create duplicate runs while an intentional rerun can use a new request key;
- a status check and a monotonically increasing `version` for compare-and-set
  updates;
- a lease on claimed work so a worker crash does not strand a run in `running`;
- an approval bound to a canonical action hash, work item, scope, approver,
  and expiry, with revocation represented as a durable state transition.

Every successful work claim increments a fencing token. Every subsequent
context, event, checkpoint, or result write must present the current token;
the repository rejects writes from an expired or superseded worker. A lease
timeout alone is insufficient because an old worker may still be running.

All work kinds use the same repository transition/event implementation; only
subtype-specific inputs and execution logic vary. Put the schema definition and
database queries in `.sql` files loaded through the Lisa repository layer, in
line with the repository SQL guardrails. The operational schema is a single
current definition (`schema.sql`); Lisa does not run upgrade migrations. Use
UTC timestamps. Event `payload_ref` and context `source_ref` should prefer
stable identifiers over copied raw session JSON. Large or sensitive tool
payloads belong in a run artifact with an access policy, not in an unbounded
event row.

## Run context and workspace layout

Allocate a fresh directory when a work item is accepted:

```text
<LISA_DATA_DIR>/work-items/<work-item-id>/
  context.md          # compact, curated facts and task-specific instructions
  checkpoint.json     # materialized checkpoint snapshot, if needed
  artifacts/          # reports and bounded trajectory extracts
  subwork/<child-id>/ # only when a child work item is created
```

The directory is private to the work item. A run receives only the policy ID,
relevant user request, selected span references, and context items Lisa
explicitly loads. It does not inherit the main chat transcript,
another run's scratch files, or private reasoning. For each artifact, Postgres
records its relative path, content hash, media type, and creation event in
`work_context` so the database and filesystem can be reconciled after
interruption.

The database stores compact facts and checkpoints that need querying or
recovery. The directory stores longer documents, extracted trajectories, and
report files. `context.md` and `checkpoint.json` are readable/materialized
projections of selected database rows, not separate authorities: if either
disagrees with Postgres, regenerate it. Avoid storing the same evolving state
independently in both places.

Long tasks can create child work items. Each child gets its own context path,
lease, and event stream, with `parent_id` pointing to the coordinating task.
Initially, an evaluation should run as one work item; subagents are an
execution option for work that is genuinely long or parallel, not a mandatory
layer. A parent sees child summaries and selected artifacts, not the child's
entire private context.

## Lisa's database interface

Provide Lisa a `lisa.db` read tool modeled on Muse's documented database
capability:

- expose the Lisa schema and table/column descriptions as a resource available
  in the agent prompt;
- accept one bounded, read-only `SELECT` against approved Lisa views and policy table;
- parse and allowlist the query shape, relations, columns, and functions; reject
  system catalogs, hidden columns, locking clauses, and functions with side
  effects or resource-control behavior (including sleep/lock functions);
- expose approved Lisa views and the policy table through explicit relation and
  column allowlists; keep other base tables hidden;
- use a read-only database role, parameter binding, and row/byte/time limits;
  the database credentials are scoped to the Lisa deployment's single
  workspace;
- return typed rows and truncation metadata, not database credentials or raw
  connection details;
- include examples for finding a run, checking its latest status, reading its
  event timeline, and finding waiting approvals.

Use typed write operations such as `create_work_item`, `claim_work_item`,
`append_work_event`, `save_context_item`, `request_approval`, and
`finish_work_item`. Each operation validates the configured workspace binding
and current fencing token and commits the state change and timeline event
atomically. Approval
resolution is deliberately not an agent tool: only the authenticated chat or
other trusted user-facing transport may record the user's decision. A generic
SQL write tool would make approval rules, idempotency, and recovery harder to
enforce.

Provision three database credentials: a deployment-only provision owner, a
Lisa runtime writer, and a Lisa runtime reader. Provisioning grants the writer
only the tables its typed repository needs and grants the reader `SELECT` on
approved `lisa.readable_*` views plus approved columns from `lisa.policies`.
The reader cannot access other base tables; neither runtime role can create or
alter tables. Configure Lisa with
`LISA_DATABASE_URL` and `LISA_DATABASE_READ_URL`, and provision the schema
separately with `LISA_DATABASE_PROVISION_URL`. The deployment operator creates
the `lisa_writer` and `lisa_reader` roles (or sets
`LISA_DATABASE_WRITER_ROLE` / `LISA_DATABASE_READER_ROLE`) before provisioning;
the agent never receives provision-owner credentials. When the schema shape
changes, re-run provisioning with `LISA_DATABASE_RECREATE=1` (drops
`lisa` and creates the current `schema.sql`)—do not add ALTER migrations.
If the database is already bound to this workspace and recreate is unset,
provisioning is a no-op.

The database query tool is for Lisa to decide what existing work to inspect
and how to resume it. It is not a way to query TheLake telemetry: trace/session
evidence continues through the existing checked query API, which enforces its
time bounds and workspace engine rules. Text from sessions, tools, files, and
connectors is untrusted evidence, never database instructions. Do not expose
hidden chain-of-thought as context; store concise decisions, evidence, and
user-visible explanations instead.

## Evaluation lifecycle

1. **Accept.** Lisa determines whether the user request or scheduled cycle
   creates a new evaluation, attaches to an existing one, or is just a
   conversational question. She creates the work item and evaluation subtype
   transactionally, using an idempotency key from the triggering request.
2. **Store trajectory inputs.** The runner stores the compact session trajectory
   in an `evaluation_inputs` context item and `inputs.json` run artifact so a
   retry does not silently re-query changed session data. Approved candidates
   are loaded from `lisa.policies`; Decisions triages all candidates and the
   Gemini judge receives only escalated policies. The policy ID, rather than a
   copied policy body, remains on the evaluation row.
3. **Claim and execute.** A worker claims the queued item with a lease, records
   a start event, loads the run inputs, and follows the production policy flow:
   retrieve approved candidates from `PolicyService`, triage all candidates
   through Decisions, then judge only escalations against the approved policy.
   Only initial preparation fetches
   bounded session detail via TheLake's existing query path. It writes
   checkpoints and meaningful events as it progresses.
4. **Approve when required.** If an action crosses the configured approval
   boundary, Lisa creates an approval request and moves the item to
   `waiting_approval`. The trusted chat transport binds the user's decision to
   that request; the model cannot call an approval resolver. Before execution,
   the executor atomically verifies the action hash, scope, expiry, revocation
   state, and authenticated approver, then claims the approval once. Evaluation
   itself can proceed automatically when the policy is
   approved and the run is read-only.
5. **Persist result.** Before calling TheLake's score mechanism, Lisa inserts
   a durable outbox intent in Postgres containing the work item, stable
   idempotency key, result payload/hash, and attempt state. The worker
   submits the score with that idempotency key (and a deterministic score ID
   where supported), then records the returned score ID and marks the outbox
   acknowledged in a fenced transaction. That transaction also updates the
   evaluation result, finishes the work item, and appends its terminal event.
   If the process crashes, recovery resubmits by the same key and does not rerun
   the judge for that persisted result. A canonical Postgres request key is
   derived from the session, policy hash, sanitized trajectory digest, model
   configuration, and pipeline version; TheLake score history is not used to
   deduplicate tracked runs. A changed trajectory creates a distinct run.
   If the score API cannot support idempotency or lookup by key, add that
   contract before enabling automatic retries.
6. **Report.** The single main chat receives a concise start/progress/result
   message with a work-item link or ID. The status and detailed trajectory are
   rendered from Postgres events and run artifacts, not reconstructed from
   duplicate tool-call messages in the chat.

For a scheduled sweep, create one `evaluation_batch` work item and one child
`evaluation` work item per policy/session pair. Each child has independent
context and a result. The batch summarizes child outcomes and can resume only
the unfinished children.

## Approval and policy boundaries

`lisa.policies` is the editable policy source, including its approval state.
Policy tools display and accept a readable Markdown form. Each rule has a
short paragraph-style ID (`P1.1`, `P1.2`, …) that remains stable when the rule
is edited. Evaluation runs keep the policy ID. The current evaluation service
loads the approved candidate set from `lisa.policies`, triages each candidate,
and passes only escalated policies to the judge.
Lisa must not silently modify policy or change approval state as a side effect
of judging. Policy approval answers whether a policy may be used for
evaluation; `approval_requests` answer whether Lisa may perform a specific
consequential action. These are separate decisions.

For a request requiring user authorization, persist the proposed action and
the exact approval request before asking in the main chat. Store the user's
response as an approval decision, bound to that request by a trusted transport.
If Lisa cannot confidently associate a reply with a pending request, she asks.
Approval state must be checked by the operation that performs the action, not
merely trusted because the model remembers seeing an affirmative message. The
check and one-time claim must use the same transaction that creates the
execution intent; an expired, revoked, consumed, or differently hashed action
must fail closed.

## UI and conversation behavior

Keep one main Lisa conversation. A run may send a brief message such as
“I started evaluation of session …” or “The evaluation finished; it found …”.
The run itself is an activity with status, a human-readable timeline, and
artifacts. The activity view should render meaningful lifecycle events and
results, rather than one card per low-level tool call. Selecting a run can show
its policy, evidence references, compact trajectory, verdict, and
approval history. Chat and activity views reference the same work-item ID.

Topic continuity in the conversation can help Lisa choose whether to create or
resume a work item, but the work-item state is authoritative once created. A
one-off question should remain a chat exchange; it does not need a work item
unless it creates follow-up, execution, or a durable evaluation record.

## Implementation status and remaining work

Lisa now has PostgreSQL provisioning for its workspace-bound operational
database, typed repositories and MCP lifecycle tools, a bounded read-only
`lisa_db` interface, and persisted work items for individual evaluations.
`EvaluationService` stores evaluation inputs, claims work with leases, writes
scores through an outbox, and can recover interrupted score writes. Policy
triage and judge execution use the production path documented in
[Lisa QA](lisa-qa.md).

The remaining product work is to make scheduled sweeps create a parent batch
and child work items, then expose their progress and outcomes as a readable
activity timeline in Lisa's main conversation and UI. The current scheduler
still performs bounded session sampling in a model turn; it does not provide a
durable resumable batch with independent child status. Richer per-run context
selection and checkpointing should be designed around the existing persisted
inputs, events, and lease recovery rather than recreating those mechanisms.

Before enabling durable scheduled batches, verify batch and child idempotency,
isolated context, event ordering, score linkage, crash recovery, approval
wait/resume where relevant, and the user-visible result. Use the production-path
evaluation and dogfood workflows as quality checks; do not treat a standalone
judge call as validation of the scheduled workflow.

## References

- The user-provided Muse examples informed the read-only schema-query and
  per-activity working-context patterns. Private example paths and contents are
  intentionally not recorded in this public repository.
- Current Lisa evaluation implementation: `lisa/src/lisa/services/evaluations.py`.
- Current Lisa workspace conversation and HTTP assembly: `lisa/src/lisa/app.py` and `controllers/http.py`.
- Current Lisa architecture notes: `docs/architecture/lisa-qa.md`.
- Project SQL safety contracts: `AGENTS.md`.
