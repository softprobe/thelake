# Lisa activities

An activity is one inspectable action Lisa performed, such as evaluating a
session, handling a conversation, or preparing a report. It records what Lisa
did through a general event timeline and optional context. An activity may
link to one or more evaluation runs, but its identity is independent of those
runs and of any session or policy.

The main chat and each Slack thread remain separate communication channels.
Postgres stores shared activity status, events, evaluation results,
and score delivery state. For each substantive web or Slack turn, Lisa decides
whether the request continues an open activity or starts distinct work. She
looks up and reads a matching activity before continuing it, creates an activity
when work is new, and explicitly attaches the chat turn to the selected record.
The conversation service then records the turn's user and assistant messages as
ordered activity events and links their message IDs. Message text is redacted
before storage, and duplicate message IDs do not add duplicate events. Greetings
and clarifications that do not start or advance work are not attached. The
activity's user-visible trajectory therefore includes the chat that started or
changed the work alongside tool actions, evidence, and lifecycle events. Larger
inputs and rendered context live under
`<LISA_DATA_DIR>/activities/<activity-id>/`. The database is authoritative for
identity and state; files are private artifacts referenced by the activity.

## Evaluation relationships

`evaluate_session` creates one activity for the action. Its event timeline
records the policy triage decisions used by that action, including cached
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
multi-step work. `evaluate_session` owns its own activity lifecycle.
`complete_activity` can finish open non-evaluation activities that have
no linked evaluation run. The kind check protects the interval before an
evaluation run exists; evaluator-managed lifecycles are completed by
`EvaluationService`.
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

For each substantive web or Slack turn, Lisa checks for a matching open
activity, reads its thread before resuming, or creates a new activity when the
user has switched work. Lisa should explicitly attach the turn to the selected
activity. As a runtime safeguard, Lisa also infers a target when the turn has
exactly one unambiguous activity ID in its activity-tool results or arguments.
Conflicting targets are left unattached unless Lisa explicitly
selects one. The runtime appends redacted user and assistant messages, tool
calls, and tool results in their original order. Message and runtime event IDs
are linked to the activity, so retries do not duplicate timeline entries.
Missing or unauthorized targets do not fail the chat turn. Greetings and
clarifications that do not advance work are left out. The Lisa activity feed
refreshes after each successful chat turn, and the activity detail dialog
shows each stored chat and tool event.
Only web or Slack turns from an actor listed in `LISA_OWNER_USER_IDS` may attach
chat content to the shared workspace activity feed. The MCP tool enforces both
the turn source and owner identity before it allows the runtime to persist
those messages and events.

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
