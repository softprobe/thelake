# Lisa activities

An activity is one inspectable action Lisa performed, such as evaluating a
session, handling a conversation, or preparing a report. It records what Lisa
did through a general event timeline and optional context. An activity may
link to one or more evaluation runs, but its identity is independent of those
runs and of any session or policy.

The main chat and each Slack thread remain separate communication channels.
Lisa's Postgres stores activity status, events, evaluation results,
and local result-journal state. For each distinct user request in a web or Slack turn, Lisa resolves an open
matching activity or creates a new one before task-specific work. Independent
requests in one message receive separate activities. A clearly matching open
request can continue; task switches, uncertain continuation, and closed work
start a new activity. Brief factual requests are included; greetings and filler
can be explicitly skipped. The conversation service records the turn's user
and assistant messages on every resolved activity, then records each tool event
on its resolved task. Within a turn, Lisa reuses the first resolver result and
stable request key for that request, including when later tools fail; it does
not create a replacement activity for the same work. Message text is redacted before storage, and duplicate
message IDs do not add duplicate events. Missing disposition falls back to a
new activity; failures after resolution mark only the affected activity as
failed. Larger inputs and rendered context live under
`<LISA_DATA_DIR>/activities/<activity-id>/`. The database is authoritative for
identity and state; files are private artifacts referenced by the activity.

## Evaluation relationships

`evaluate_session` records one activity for the action. In chat it reuses the
active request activity resolved for the turn; pushed session traces and
explicit evaluation requests create an evaluation activity directly. Its event timeline records the policy triage decisions used by that action, including cached
negatives. Lisa judges only escalated policies. A no-violation decision
remains visible in the event timeline and does not create a pass score.
Decisions provider attempts are linked to the same activity record, and a
failed attempt is recorded there without creating a cache entry.

The completed activity summary is the user-facing aggregate outcome. It names
every policy whose judge result is `fail`, while `evaluation_runs` remains the
source for each policy's verdict and rationale. If no judge finds a violation,
the summary distinguishes reviewed policies, policies cleared by triage,
insufficient evidence, and the absence of approved policies. Activity success
describes completed processing; the summary carries the policy finding.

| Relation | Responsibility | Identity |
|---|---|---|
| `lisa.activities` | Inspectable Lisa action and overall lifecycle | `id` |
| `lisa.evaluation_triage` | Durable pair-only Decisions cache used to filter later calls | `(session_id, policy_id)` |
| `lisa.evaluation_runs` | Judge result linked to the activity that requested it, deduplicated across actions by request identity | `id`; unique `(policy_hash, session_id, request_key)` |
| `lisa.score_outbox` | Exact locally staged result payload used to recover and finalize one evaluation result | `id`; unique `idempotency_key`; references `evaluation_id` |
| `lisa.activity_events` | Ordered timeline of Lisa's actions and outcomes | `(activity_id, seq)` |
| `lisa.activity_context` | Evaluation inputs and immutable chat-activity memory snapshots | `id`; ordered by `(activity_id, seq)` |

The same deduplicated evaluation result may be returned during a later activity
without creating another judge result; that activity records the reuse in its
event timeline. The outbox is Lisa-local result staging and recovery state. It
does not deliver scores to TheLake or another centralized service.

The read-only `lisa.readable_activities`, `lisa.readable_activity_events`,
feed, monitor, and link views, `lisa.readable_activity_context`, and
`lisa.readable_evaluations` expose
the inspection surface through `lisa_db`. The evaluation view reads actual
judge runs and their linked activity, while activity events capture Lisa's
actions in order. The writer uses typed repository methods; the reader cannot
access base tables or modify state.

## Feed and monitor contract

Lisa's `activities` and `activity_events` remain canonical.
`activity_feed_entries`
stores card metadata such as type, goal flag, subtitle, task label, agent and
details. `activity_monitor_threads`, `activity_monitor_thread_sections`, and
`activity_monitor_thread_actions` provide the thread surface. The four link
families map agent, message, runtime-work, and carrier-message IDs to activity
threads. Activity events are mirrored into monitor actions in the same database
transaction; terminal Lisa states map to feed statuses `success`, `error`, or
`stopped`, while queued/running map to `pending` and waiting states to `blocked`.

The MCP tools expose activity search, filtered feed queries, progress and next
action updates, sections, linked references, and completion. Lisa's packaged
`manage-lisa-activities` skill defines when she finds, creates, updates, and
completes records; the runtime system prompt directs her to it for substantial
multi-step work. `evaluate_session` owns the evaluation timeline and reuses the
active chat activity when one has been resolved. For a web or Slack chat, the
service defers terminal lifecycle changes to the conversation runtime. The
runtime first writes the final assistant message and tool result, then freezes
the activity at that resulting event watermark. Pushed-session evaluation
remains service-owned and completes after its evaluation runs finish.
`complete_activity` can finish open non-evaluation activities that have
no linked evaluation run, except activities resolved for the current web or
Slack chat turn. The chat runtime owns those activities' finalization after
Lisa's reply, so a direct tool call cannot close it before pending work is
classified. The kind check protects the interval before an evaluation run
exists. Pushed-session evaluation lifecycles are completed by
`EvaluationService`; chat-attached evaluation results are finalized by the
conversation runtime after the same turn's full trajectory is stored.
After a successful owner chat reply, the conversation runtime completes any
still-open chat request. It leaves work in `waiting` when tool arguments or
results record an outstanding next action, clarification, or policy approval.
A `queued` or `running` progress update with a non-empty `next_action` remains
open in the feed as `blocked` with that action; an explicit terminal
`complete_activity` result closes it.
An explicit deferred `complete_activity` outcome is applied to the activity ID
in that call, including when that activity was waiting on an owner action that
the owner completed in a later turn. Explicit activity IDs keep completion
evidence scoped when a turn handles several requests. Read-only activity-thread
evidence is not considered a failed tool call when its serialized history
contains an older error; failure detection uses the current result envelope.
`evaluate_session` validates the session before the
no-approved-policies shortcut. On lookup failure for a resolved chat activity,
it records a failed timeline event and returns `activity_finalized: false`;
the conversation runtime stores the tool result and reply, then finalizes the
request as failed. The agent must not retry completion or perform a second
lookup. Non-chat evaluations continue to finalize through `EvaluationService`.
Pushed-session evaluation failures remain retryable until the queue exhausts its
delivery limit. Each failed delivery updates the activity summary and exposes a
bounded, redacted error in its timeline while the activity remains pending. The
worker tags service-written failures with the delivery key and enriches the same
event, avoiding a second failure entry; failures outside the evaluator receive
one idempotent delivery event. A later successful retry replaces the current
retry warning with a recovery summary. Exhaustion records the last redacted
reason and marks the activity failed only when no live evaluation lease or
pending score outbox owns it. Lisa's activity card shows the retry error before
the owner opens its timeline; an open activity panel and selected timeline
refresh every ten seconds so new failures appear without a manual refresh. If
activity storage is unavailable, the worker keeps queue custody and reports the
persistence failure in its service log; the UI can display it after the activity
write succeeds.
Unclaimed finalization can close queued or resumed waiting work, but only when
no live worker lease owns it; an active evaluator must finish with its fencing
token. This preserves one lifecycle owner across chat continuations and worker
retries.
The read-only `lisa_db` resource documents matching views. Lisa's own frontend
renders its activity feed; MCP and database views are Lisa's inspection
surfaces. TheLake Explorer remains a separate trace-inspection application and
does not proxy Lisa workspace activity. Its Lisa panel's Recent evaluations
list combines policy-evaluation activities with pushed-session evaluation
activities from the bounded activity feed. A separate Policy findings tab
searches those evaluation activities for policy-violation summaries before the
feed limit, then filters to reported findings;
finding cards name the policy and use an alert treatment. Evaluation cards also
show a finding badge when applicable. The activity's `success` status means
processing completed and does not mean the policy passed. Presentation updates
cannot set lifecycle status. The resource's relation and column list is
authoritative for SQL
diagnostics; do not probe unsupported fields by selecting them. Report a field
as unavailable when it is absent from the documented schema.

The main chat and each Slack thread keep independent transcripts but share
activity IDs. Lisa serves its own activity and chat APIs from its standalone
Python service; the Explorer service does not own or expose Lisa activity data.

Policy lifecycle state lives in `lisa.policies`: readable policy IDs, criteria,
and approval state are the complete source of truth. The table has no
`archived_at` field; deletion removes its row. Explicit database diagnostics may
read the table through `lisa_db` using its approved columns, while policy
workflows use the policy tools. A policy record has no activity foreign key;
substantive policy chat and actions can be recorded in a generic activity
timeline. A policy update keeps its policy ID and row in place.
Changed criteria invalidate
that policy's cached triage decisions, while stored evaluation outputs retain
the criteria used for their original judgment. If an approved policy changes,
its `approved` value becomes false until an authorized owner asks Lisa to
approve that same ID. Lisa calls `approve_policy` with that ID and approves the
same row. Policy lifecycle state remains on the policy row.
While a policy created or updated by the owner is unapproved, its chat activity
stays open in `waiting` with owner approval as its next action. A later approval
continues that activity across web and Slack until the policy is approved.
Lisa closes the activity after the requested policy workflow reaches its
terminal state.
If an approval attempt fails while the activity is already waiting, the
activity keeps its existing next action instead of being closed as a failed
chat request.

Policy approval checks the authenticated owner on a web or Slack turn. The
policy ID is the `approve_policy` argument.
Configure `LISA_OWNER_USER_IDS` with comma-separated identity subjects; web
turns use the signed assertion subject and Slack turns use the Slack user ID.
Workspace and channel access alone do not authorize policy approval. Configure
matching identifiers for each channel the owner uses.

## Triage and retry behavior

Before calling Decisions, `EvaluationService` filters approved policies using
the exact pair key `(session_id, policy_id)` in `lisa.evaluation_triage`. Only
pairs without a stored decision are sent to Decisions. A complete response is
validated and persisted before judging. The activity event timeline records
the decisions used for that action, whether freshly triaged or loaded from the
pair cache.

Pair-only caching is intentional: telemetry changes, policy edits, and triage
model or prompt changes do not invalidate a stored decision for the same pair.
Failed or incomplete Decisions responses create no cache row. Concurrent first
calls may both reach Decisions; the first persisted decision is canonical.

Each escalated pair retains its existing evaluation request identity and local
result outbox. A pending result is recovered from its stored payload and
idempotency key; Lisa does not judge again after the result payload exists. The
activity finishes after its evaluation runs have terminal results and the local
result records are acknowledged.

## Chat activity trajectory

Each distinct request in a web or Slack turn is resolved with
`resolve_chat_activity` before task-specific work. Lisa uses a stable request
key for each intent, continues only an inspected matching open activity, and
creates a new activity for new work, task switches, uncertain continuation, or
closed activities. Independent asks map to independent activities. Factual
one-turn asks are included; `skip_chat_activity_for_turn` is reserved for
greetings and conversational filler.
When the owner explicitly asks to create or track new work, Lisa calls the
resolver with `disposition="create"` without first searching or listing
activities. It sets progress once if work remains and uses the activity's next
action instead of creating a parallel checklist.
If that is the requested outcome, Lisa records the work but does not perform
the investigation itself. Words such as "investigation" or "agent" in the
activity title do not authorize session, database, policy, or evaluation search.

For a continuation, the model must complete the lookup as one sequence:
`find_activity` → `get_activity_thread` → `resolve_chat_activity`. It must not
read or write the checklist, list recent activities, or call another tool
between those steps. A remembered activity ID does not replace the current
lookup. Resolution binds the current turn's tool events to the matching work;
without it, the runtime creates a separate fallback activity for the turn. The
checklist is separate persistent context and is used after the relevant
activity has been resolved.
Successful activity write responses are the confirmation for those writes;
Lisa does not reread the thread just to verify a successful update. It reads a
thread for a continuation or when the owner asks for activity history.
The agent-facing MCP response is an allowlisted projection with at most 12 recent
actions, 12 sections, and 24 references, plus a 32 KiB serialized-payload cap.
Action summaries are capped at 1,200 characters. Arbitrary detail JSON and unknown
nested fields are omitted; omission and truncation counts are returned. This keeps
tool output within transport limits without truncating the persisted timeline or
user-visible thread. After resolving a continuation, Lisa uses
`load_activity_memory` for historical task context and its event references.
An `update_activity` call that appends a timeline action must provide both
`event_type` and `event_title`; presentation-only updates omit both.

Resolver summaries are routing control data and are excluded from activity
memory because they can accidentally include another request's facts. When a
verified fact, decision, or outcome should remain available after compaction,
Lisa records a scoped timeline event after loading that activity's memory and
before switching to another activity. Use `update_activity` with a task-note
event or `set_activity_progress` when the fact belongs in progress state. Keep
each event limited to the selected activity; shared user and assistant messages
are not a durable substitute, especially when one turn resolves multiple tasks.

The resolver result selects and scopes the activity, but its arguments and
result remain control data in the global conversation trace. Activity thread
and memory projections do not persist resolver payloads as task evidence,
because generated titles and summaries can mix tasks. The runtime links the
turn's redacted user and assistant messages to every activity resolved for that
turn and partitions later tool calls and results by the most recent resolved
request or explicit activity ID. Thus a shared message can appear on multiple
related work records while each task evidence trace contains only its scoped
work. Missing resolution receives one fallback activity unless Lisa explicitly
skips the turn. The fallback gets a generated routing receipt in the chat
timeline so the missing resolver result is visible to the owner and
distinguishable from Lisa's reply. A fallback means the model did not return a
valid resolver result; it is a routing defect to investigate, not a normal
resolution path.
Failures after resolution preserve the trajectory and fail only that activity;
owner chat failures before resolution create a visible failed request activity.
Trajectory persistence and the resulting waiting or terminal lifecycle update
commit in one Postgres transaction, so a card cannot retain a partial chat
trajectory after a failed completion write. Persistence errors surface to the
turn boundary instead of being acknowledged as successful. Resolver and tool
replays within one runtime turn are idempotent through deterministic
turn/request identity and unique source-event links. Every new HTTP submission
gets a new turn identity, so resubmitting after losing a response may create a
second turn. The chat API does not accept a durable cross-request idempotency
key.

Only owner web or Slack turns may copy chat content to the shared activity feed.
The MCP resolver enforces source and owner identity. The feed refreshes after a
successful turn, and the activity detail view displays ordered chat and tool
actions with their linked message and runtime-event IDs. This feature records
work initiated in chat; it does not claim
unprompted discovery of every user's work.

## Per-activity context memory

Lisa's single main chat retains one ordered transcript and one global
conversation checkpoint. Each resolved chat activity additionally has its own
versioned memory snapshot. The model must explicitly resolve the current task
before loading memory; the runtime does not classify a user turn to choose a
task. The loader authorizes only the resolved activity or its direct completed
parent. A closed parent remains read-only, and follow-up work is recorded in a
new linked child activity. After every resolution, load the selected activity's
memory immediately, including when it is newly created and the result is empty
or pending. The MCP server rejects owner chat writes and session evaluation
until memory for the exact resolved activity has been loaded. For a child based
on completed work, load the child first, then its direct parent when the answer
depends on that history. Namespace discovery and loading may be required before
the activity tools are visible; those schema setup calls do not change the
activity sequence. OpenHands advertises the packaged Lisa skill and loads its
full instructions through `invoke_skill` using the exact listed name. The agent
then uses Lisa MCP tools to read or change activity state; skill invocation
does not select or update an activity.

The chat trajectory transaction enqueues snapshot work with its committed
event watermark. When a turn becomes multi-activity, resolution also enqueues
the new neutralized privacy generation for every mapped activity, even when
an activity's event watermark did not change. Snapshot identity includes
`title_neutralized` alongside activity, watermark, and configuration, so a
safe immutable replacement can publish at the same watermark. The shared
conversation checkpoint worker calls the common summary-generation contract
and publishes snapshots through the chat-owned repository method in
`activity_context`. Evaluation context writes retain their existing lease and
fencing-token checks. Concurrent or redelivered jobs within one privacy
generation reuse an existing artifact. Publication is monotonic within that
generation. A terminal snapshot is accepted only for the exact watermark of
the completed activity.

The summary carries a prompt/schema version, content hash, model/prompt
configuration fingerprint, event watermark, and resolvable event references.
References are validated against the activity's own event prefix before
publication. Summaries and event content are untrusted data, not instructions.
When the latest snapshot lags, the loader returns `pending` plus an ordered
suffix limited to 80 events and 12,000 serialized text characters, with current
and snapshot watermarks. `fallback_truncated` indicates that the suffix was
shortened. Lisa may use that suffix while generation is pending and must
describe the memory as incomplete. An empty snapshot is not proof that the
activity has no history.

For a chat turn that resolves multiple activities, the event store retains
turn-tagged resolver, transcript, chat-level progress, and runtime-tool events
for traceability. Thread and memory projections suppress resolver payloads and
other shared copies; resolver summaries are not presumed task-scoped. For
`progress` events written earlier in the same turn, it neutralizes the title
that may have copied an untrusted resolver title. It also withholds raw
`update_activity` arguments that can repeat a contaminated title. Progress
summaries and task notes written to the exact activity remain available.
The second activity resolution also replaces shared-turn titles and summaries with neutral labels
such as `Task 1` in activity, feed, monitor, and thread-action projections. The
original task wording remains a private feed search key but is never returned
in read results. A durable activity flag keeps later title and presentation
writes from restoring public title, subtitle, label, or details text. Activity
snapshots published before the second resolution are ignored by comparing the
captured neutralization flag, including when the old title already matched the
neutral label. The completed shared-turn trajectory is sanitized again after
its completion writes. Writes explicitly addressed to one activity remain
available to that activity. Keep each progress `next_action` limited to the
next step for its resolved activity; do not copy another request's facts or
work from the shared transcript into its progress state.
Activity listing, lookup, thread, memory, and database reads require an
authorized owner.
Completed chat activities are locked against event, presentation, section, and
reference writes; only a new linked child can receive follow-up work.
The MCP response strips the internal chat-turn linkage field. Snapshot state is
additional context; the ordered activity event log remains the source of truth.

## Scope and storage

One Lisa deployment is bound to one workspace. Keep its Postgres database
separate from TheLake's DuckLake catalog and bind it to the configured workspace
fingerprint. Policies remain in `lisa.policies`; policy tools and
`PolicyService` own policy changes and approvals. Evaluation inputs use the
normal session and policy services and never accept dataset-selected policy
objects directly.

Provision the Lisa schema with `lisa/scripts/provision_lisa.py`. Lisa uses one
clean schema definition without migrations or compatibility paths. Explicit
schema recreation drops and rebuilds the Lisa schema, clearing its policies,
activities, and operational history. Automated tests use transient Lisa
databases and must not connect to a developer's running service or database.
