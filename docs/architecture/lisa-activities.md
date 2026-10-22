# Lisa activities

An activity is one inspectable action Lisa performed, such as evaluating a
session, handling a conversation, or preparing a report. It records what Lisa
did through a general event timeline and optional context. An activity may
link to one or more evaluation runs, but its identity is independent of those
runs and of any session or policy.

The main chat and each Slack thread remain separate communication channels.
Postgres stores shared activity status, events, evaluation results,
and score delivery state. For each distinct user request in a web or Slack turn, Lisa resolves an open
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

`evaluate_session` owns one activity for the action. In chat it reuses the
active request activity resolved for the turn; scheduled invocations create an
evaluation activity directly. Its event timeline records the policy triage decisions used by that action, including cached
negatives. Lisa judges only escalated policies. A no-violation decision
remains visible in the event timeline and does not create a pass score.
Decisions provider attempts are linked to the same activity record, and a
failed attempt is recorded there without creating a cache entry.

| Relation | Responsibility | Identity |
|---|---|---|
| `lisa.activities` | Inspectable Lisa action and overall lifecycle | `id` |
| `lisa.evaluation_triage` | Durable pair-only Decisions cache used to filter later calls | `(session_id, policy_id)` |
| `lisa.evaluation_runs` | Judge result linked to the activity that requested it, deduplicated across actions by request identity | `id`; unique `(policy_hash, session_id, request_key)` |
| `lisa.score_outbox` | Exact pending score payload for one evaluation result | `id`; unique `idempotency_key`; references `evaluation_id` |
| `lisa.activity_events` | Ordered timeline of Lisa's actions and outcomes | `(activity_id, seq)` |
| `lisa.activity_context` | Inspectable activity inputs and checkpoints | `id`; ordered by `(activity_id, seq)` |
| `lisa.activity_followups` | Owner-approved due-time or event-triggered continuation | `id`; one-shot claimed status |

The same deduplicated evaluation result may be returned during a later activity
without creating another judge result or score; that activity records the reuse
in its event timeline. Score delivery remains attached to each evaluation
result, so it can retry its own stored payload and idempotency key.

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
multi-step work. `evaluate_session` owns the evaluation lifecycle and reuses the
active chat activity when one has been resolved.
`complete_activity` can finish open non-evaluation activities that have
no linked evaluation run, except activities resolved for the current web or
Slack chat turn. The chat runtime owns those activities' finalization after
Lisa's reply, so a direct tool call cannot close it before pending work is
classified. The kind check protects the interval before an evaluation run
exists; evaluator-managed lifecycles are completed by `EvaluationService`.
After a successful owner chat reply, the conversation runtime completes any
still-open chat request. It leaves work in `waiting` when tool arguments or
results record an outstanding next action, clarification, policy approval, or
follow-up confirmation. A `running` progress update with a non-empty
`next_action` remains open in the feed with that action; an explicit terminal
`complete_activity` result closes it.
An explicit deferred `complete_activity` outcome is applied to the activity ID
in that call, including when that activity was waiting on an owner action that
the owner completed in a later turn. Explicit activity IDs keep completion
evidence scoped when a turn handles several requests. Read-only activity-thread
evidence is not considered a failed tool call when its serialized history
contains an older error; failure detection uses the current result envelope.
`evaluate_session` validates the session before the
no-approved-policies shortcut. On lookup failure for a resolved chat activity,
it records a failed timeline event and reports whether it finalized the
activity. `activity_finalized: false` means no finalization occurred; the agent
must not retry completion because the result may reflect a concurrent or
already-terminal lifecycle. The agent reports the returned error without a
second lookup or completion call.
Unclaimed finalization can close queued or resumed waiting work, but only when
no live worker lease owns it; an active evaluator must finish with its fencing
token. This preserves one lifecycle owner across chat continuations and worker
retries.
The read-only `lisa_db` resource documents matching views. Explorer renders the
user-facing activity feed; MCP and database views are Lisa's inspection
surfaces. Presentation updates cannot set lifecycle status.

The main chat and each Slack thread keep independent transcripts but share
activity IDs. Explorer exposes the workspace feed and activity timeline through
authenticated `/v1/activities` endpoints.

`activity_followups` stores owner-confirmed one-time follow-up requests. Lisa may
suggest a due check, but the pending record does not run until the owner confirms
`Confirm follow-up: <id>` in either chat. The scheduler claims due rows with
`FOR UPDATE SKIP LOCKED`, resumes the linked activity through the persistent
main conversation, and posts the result to `LISA_SLACK_ACTIVITY_CHANNEL_ID` and
optional `LISA_SLACK_ACTIVITY_THREAD_TS`. Failed turns release their claim for
retry. Scheduled turns cannot write policy.

Policy lifecycle state lives in `lisa.policies`: readable policy IDs, criteria,
and approval state are the complete source of truth. A policy record has no
activity foreign key; substantive policy chat and actions can be recorded in a
generic activity timeline. A policy update keeps its policy ID and row in place.
Changed criteria invalidate
that policy's cached triage decisions, while stored evaluation outputs retain
the criteria used for their original judgment. If an approved policy changes,
its `approved` value becomes false until the owner sends
`Approve policy: <policy_id>` as an exact standalone command. Lisa approves the
same row. Policy lifecycle state remains on the policy row.
While a policy created or updated by the owner is unapproved, its chat activity
stays open in `waiting` with owner approval as its next action. A later approval
continues that activity across web and Slack; extra words leave it waiting.
Lisa closes the activity after the requested policy workflow reaches its
terminal state.
If an approval attempt fails while the activity is already waiting, the
activity keeps its existing next action instead of being closed as a failed
chat request. A confirmed follow-up stays blocked with `Wait for approved
follow-up` until the scheduled work reaches a terminal state.

Owner confirmation checks both the exact command and the authenticated actor.
Configure `LISA_OWNER_USER_IDS` with comma-separated identity subjects; web
turns use the signed assertion subject and Slack turns use the Slack user ID.
Workspace and channel access alone do not authorize policy or follow-up
approval. Configure matching identifiers for each channel the owner uses.

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

Each escalated pair retains its existing evaluation request identity and score
outbox. A pending score is retried from its stored payload and idempotency key;
Lisa does not judge again after the outbox payload exists. The activity finishes
after its evaluation runs have terminal results and acknowledged score writes.

## Chat activity trajectory

Each distinct request in a web or Slack turn is resolved with
`resolve_chat_activity` before task-specific work. Lisa uses a stable request
key for each intent, continues only an inspected matching open activity, and
creates a new activity for new work, task switches, uncertain continuation, or
closed activities. Independent asks map to independent activities. Factual
one-turn asks are included; `skip_chat_activity_for_turn` is reserved for
greetings and conversational filler.

The resolver call and its result begin the activity-scoped trajectory. The
runtime links the turn's redacted user and assistant messages to every activity
resolved for that turn and partitions later tool calls and results by the most
recent resolved request or explicit activity ID. Thus a shared message can
appear on multiple related work records while each tool trace remains scoped to
its request. Missing resolution receives one fallback activity unless Lisa
explicitly skips the turn. The fallback is labeled in the chat timeline so the
missing resolver result is visible to the owner and distinguishable from
Lisa's reply. A fallback means the model did not return a valid resolver result;
it is a routing defect to investigate, not a normal resolution path.
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
