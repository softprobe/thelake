# Lisa activities

An activity is one inspectable action Lisa performed, such as evaluating a
session, handling a conversation, or preparing a report. It records what Lisa
did through a general event timeline and optional context. An activity may
link to one or more evaluation runs, but its identity is independent of those
runs and of any session or policy.

Lisa's main conversation remains the communication channel. Postgres stores
activity status, events, evaluation results, and score delivery state. Larger
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

The MCP tools `create_activity`, `update_activity`, `complete_activity`,
`list_recent_activities`, `get_activity_thread`, `set_activity_section`, and
`link_activity_reference` expose the agent contract. Lisa's packaged
`manage-lisa-activities` skill defines when she finds, creates, updates, and
completes records; the runtime system prompt directs her to it for substantial
multi-step work. `evaluate_session` owns its own activity lifecycle.
`complete_activity` can finish only queued non-evaluation activities that have
no linked evaluation run. The kind check protects the interval before an
evaluation run exists; evaluator-managed lifecycles are completed by
`EvaluationService`.
The read-only `lisa_db` resource documents matching views. There is no Lisa
frontend renderer in this repository; these tools and views are its published
feed/thread API contract. Presentation updates cannot set lifecycle status.

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
