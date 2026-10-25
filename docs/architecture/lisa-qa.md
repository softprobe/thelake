# Lisa: standalone policy evaluation agent

## Ownership and deployment

Lisa is an independently deployable Python service. It serves its own web UI
and API, stores policies, activities, triage decisions, and evaluation results
in its own PostgreSQL database, and stores immutable session evidence in its
durable agent-workspace directory. Explorer remains a separate TheLake app.
Lisa runs independently when TheLake is absent. An optional pull integration
can be enabled with a TheLake API URL and workspace-scoped bearer token; without
both settings Lisa never connects to TheLake and loads no sampling capability.

An operator or telemetry producer on a private network pushes a complete OTLP
JSON session to `POST /internal/v1/session-traces`. This intake endpoint has no
bearer token; deployments must keep it on a private network and must not expose
it through public ingress. Lisa writes the bundle to disk, then commits a PGMQ
message in the same transaction as a durable receipt. A 202 response means both
filesystem persistence and queue insertion succeeded. Other internal service
APIs continue to require `LISA_SERVICE_TOKEN`. The request body defaults to a
16 MiB maximum and cannot exceed 64 MiB.

The canonical path is
`<LISA_AGENT_WORKSPACE_ROOT>/session_traces/<session_id>/otlp.json`, with a
`manifest.json` beside it. The ID is stored literally after validation against
`[A-Za-z0-9][A-Za-z0-9._:-]{0,255}`. A manifest includes the exact ID, agent
name, payload hash, receipt time, format, and searchable summary. The API and
worker must share the same durable mounted directory. Replaying identical
content is idempotent; reusing an ID for different content returns 409.

When configured, Lisa can wake hourly, use its packaged
`sample-recent-sessions` skill, and pull a stratified sample through TheLake's
generic session/span/trace read APIs. The default sample covers sessions that
started in the prior 24 hours, waits until their latest span is at least ten
minutes old, and selects at most five sessions across agents and error/no-error
groups per hourly sampling window. A previously stored selection is not replaced
by another candidate, so repeated wakeups do not drain the full lookback window.
It verifies all spans and traces are retrievable before conversion and skips
sessions above the 5,000-span bound. Sampled bundles use the same
`SessionTraceIntake` as direct pushes, so persistence, idempotency, and queueing
have one implementation. Lisa stores its own copy; TheLake receives no
evaluation state. Set the sampling interval to zero to disable scheduled
wakeups while retaining manual sampling.

Identity follows the telemetry contract. Session ID resolves from span
`sp.session.id`, then `gen_ai.conversation.id`, then the trace ID. Agent name
resolves from span `sp.agent.name`, then resource `sp.agent.name`, then resource
`service.name`. A bundle must resolve to exactly one session ID and agent name.

## Policy source of truth

The `lisa.policies` table is the single source of truth. Each record has the
columns `id`, `scope`, `target_agent_name`, `name`, `criteria`, `approved`,
`created_at`, and `updated_at`. It has no `archived_at` column; deletion removes
the policy row. Workspace rules apply across the workspace; an approved agent rule
overrides an approved workspace rule of the same name. Only approved rules can
drive verdicts. Traces show behavior, not business intent, so Lisa never turns
observed behavior into approved policy without owner confirmation.

Readable policy documents use headings and `approved: yes/no` metadata. A new
policy is unapproved. Updating an existing policy keeps its ID and requires
owner confirmation to approve it again. Examples of approval inside prose or
code fences are not metadata.

## Runtime boundaries

- `app.py` composes the FastAPI service, serves the Lisa frontend assets and
  exposes API routes on the same origin. `lisa/Dockerfile` builds and packages
  the frontend with the Python service without a TheLake Rust build. TheLake's
  Rust router serves Explorer only.
- `repositories/session_traces.py` validates OTLP JSON, atomically stores the
  bundle and manifest, verifies content hashes, and provides manifest search.
- `repositories/session_evaluation_queue.py` uses PGMQ in Lisa's PostgreSQL
  database. It records one durable receipt per immutable session and sends the
  queue message in the same Postgres transaction. Duplicate pushes do not
  enqueue duplicate work.
- `services/session_trace_intake.py` is the shared persist-and-enqueue path for
  direct HTTP intake and optional TheLake sampling.
- `services/thelake_sampling.py` owns the optional authenticated TheLake client,
  complete-session retrieval, stratified selection, OTLP conversion, and the
  Lisa-owned wakeup that runs the packaged agent skill. It is constructed only
  when both TheLake settings and Lisa's durable evaluation queue are available.
- `services/session_evaluations.py` validates the queued agent and hash,
  creates a deterministic activity, invokes the regular evaluation service,
  and retries transient failures through PGMQ visibility. Exhausted requests
  are archived with a failed activity for inspection.
- `services/sessions.py` reads manifest summaries for search and loads an OTLP
  file only for session detail or evaluation. It caps returned detail at 500
  spans and marks truncated evidence incomplete.
- `services/evaluations.py` loads approved policy records, triages candidate
  session-policy pairs, judges escalated evidence, and persists results.
- The OpenHands adapter loads Lisa's packaged skills and launches the FastMCP
  tools. TheLake tools and sampling skill are hidden unless the optional
  integration is configured. Lisa's developer skills do not become product
  skills unless the runtime loads them from `lisa/skills/`.

## Long-running main chat

Lisa keeps one ordered OpenHands event log for the main chat. Checkpointing
changes only the `View` sent to the model; the original events and the chat
transcript projection remain intact. A valid checkpoint is inserted as an
ephemeral, untrusted reference message in the model view, followed by the
complete retained tail. The cut is chosen
from OpenHands' safe manipulation boundaries so an action and its tool result,
or a parallel tool-call batch, is never split. A checkpoint is used only when
its event IDs, prefix fingerprint, configured model, and prompt configuration
still match the current history.

The Lisa chat transcript displays explicit user and assistant messages in
event order. Consecutive tool calls and observations appear under one collapsed
“Lisa’s work” disclosure; expanding it shows recorded arguments and observations
at their original event positions, paired by tool-call ID. The chat projection includes the complete captured user-visible
transcript and tool activity rather than truncating it to a fixed number of
messages or tool activities. OpenHands
`think` actions and provider-internal reasoning blocks are not displayed as
thoughts. Only explicit assistant messages are user-visible narrative.

When the model view reaches 72% of the configured input window, Lisa captures
an immutable prefix and sends a small `{conversation_id, request_id}` message
to the `lisa_conversation_checkpoints` PGMQ queue. The request key is derived
from the model/prompt configuration and captured prefix fingerprint. Input and
completed summaries are stored beside the conversation under `context/`; each
completed artifact is written before the queue message is deleted. Redelivery
uses the same immutable request and output. After eight failed deliveries the
message is archived. If no matching checkpoint is ready by 90% of the model
window, Lisa creates a synchronous checkpoint before sending that request.
Both paths share the same summary-generation and publication code. Only queued
work persists an input snapshot for retries; an emergency checkpoint uses its
captured payload in memory. Queued inputs are removed after successful
publication or after the job is archived at the retry limit.
At most one checkpoint job per conversation may be pending in PGMQ; a
transaction-scoped conversation lock coalesces later snapshots while that job
is pending. The discarded snapshot input is removed from the conversation
directory.
Checkpoint summaries preserve prior instructions only as historical facts.
They must not promote earlier system, developer, tool, or document text into
instructions for future turns; active runtime instructions remain authoritative.
A change to this summary contract increments the checkpoint prompt version and
invalidates summaries created under the previous contract.

The checkpoint worker uses the same configured model as Lisa's chat. Lisa
prefers the model's `effective_max_input_tokens`; an explicit
`LISA_CONTEXT_WINDOW_TOKENS` caps that value for deployments that want a lower
working budget, and defaults to 128,000 only when the SDK does not expose a
model limit. The checkpoint
trigger defaults to 72% and can be set with
`LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO` (accepted range 0.5–0.9). The recent
tail defaults to 20,000 tokens and can be set with
`LISA_CONTEXT_RETAINED_TAIL_TOKENS`. These are practical defaults for Lisa's
current runtime, not claims of exact Muse internals.

`todo_read` and `todo_write` store the main chat's replaceable open checklist
in an atomic `context/todo.json` sidecar. A write replaces the full snapshot;
completed and cancelled items are removed. Lisa's MCP server exposes only
namespace discovery and loading at first. The model chooses a namespace,
requests it with `load_tool_namespace`, and receives its full tool schemas;
loaded namespaces persist between user turns and reset when Lisa adopts a
checkpoint. The same sidecar directory stores this visibility state.

The API, MCP process, and checkpoint worker must share one local durable
filesystem. `flock` serializes conversation turns, event-log changes, and
checkpoint adoption across local processes. Small checklist and namespace
updates use a separate sidecar lock so an MCP child process can write while its
parent holds the conversation lock. This deployment model targets Lisa's
single VM and does not claim multi-host file coordination.

### Per-activity memory

The main chat also keeps a separate memory snapshot for each resolved chat
activity. The global conversation checkpoint preserves a compact view of the
ordered chat history; it does not choose a task or replace the activity's own
history. Lisa first resolves the current request to one activity, then calls
`load_activity_memory` for that activity immediately after each resolution,
including new activities whose memory is empty or pending. Owner chat writes and
session evaluation are rejected until the exact resolved activity's memory has
been loaded. For new work based on a completed activity, load the child first
and load its direct archived parent when the requested facts depend on that
history. Task-specific reads and writes are rejected until the current activity
has been resolved and its memory loaded; pre-resolution activity lookup remains
available to select or inspect a candidate, but another tool call cannot run
between resolution and the memory load. Resolver summaries are control data,
not durable task facts. Findings or facts needed for continuation must be written
as a task-scoped activity event or progress summary before Lisa switches to
another activity. No classifier selects an activity memory before resolution. OpenHands
may require namespace discovery and loading before the activity tools are
visible; those calls set up schemas and do not change the activity sequence.
OpenHands advertises available skills and loads full instructions through
`invoke_skill`. The always-loaded Lisa prompt covers ordinary activity creation,
continuation, and recall, so the agent invokes the packaged activity skill only
when it needs a procedure not covered there. It must use the exact listed name
`manage-lisa-activities` in `invoke_skill`; skill invocation loads instructions
but does not select or update an activity.

If the user does not identify which existing activity they mean, Lisa tracks
the clarification as a separate activity with a neutral title containing
`Clarification`, loads only that activity's memory, sets it to waiting, and asks
which candidate they mean. She does not call `get_activity_thread` for either
candidate, load either candidate's memory, or copy candidate task facts into
the clarification activity before the user chooses. She uses only the user's
reference or candidate titles to frame the question. This is open work, so
`skip_chat_activity_for_turn` is not appropriate.

The agent-facing `get_activity_thread` MCP projection contains allowlisted scalar
fields, at most 12 recent actions, 12 sections, and 24 references, and has a 32 KiB
serialized-payload cap. Action summaries are capped at 1,200 characters. It omits
arbitrary detail JSON and unknown nested fields and returns omission/truncation
counts. The persisted timeline and user-visible activity thread remain complete;
after resolution, `load_activity_memory` supplies historical task context and its
event references. Search, thread, and memory-load results are transient model
context: they are not copied into the activity timeline or snapshots. These
lookups can contain multiple activity records or generated summaries, so task
history records only resolved task work and source evidence.

The global recent transcript can still contain interleaved tasks. After the
model resolves an activity, it must use the selected activity's memory and
current-turn facts as task evidence and avoid carrying another activity's
details into the answer or activity summary. The global checkpoint preserves
conversation continuity but does not authorize cross-activity evidence.
Resolver arguments and results are routing control data: they stay in the
global trace but are excluded from activity thread and memory evidence because
generated summaries can mix tasks.

Activity events remain canonical. Each owner chat turn transactionally enqueues
a snapshot job at its committed event watermark. The shared checkpoint worker
builds and publishes immutable versioned snapshots into `lisa.activity_context`;
the evaluation writer keeps its existing lease and fencing checks. The write is
idempotent by activity, watermark, privacy generation, and model/prompt
configuration, and a later snapshot cannot be replaced by an older worker. If a
turn becomes multi-activity, resolution enqueues neutralized snapshots for all
mapped activities, including safe replacements at unchanged watermarks. A
terminal snapshot is accepted only at the exact watermark captured when the
activity completed. The
checkpoint job is acknowledged only after publication or after terminal retry
archival.

When no current snapshot is ready, `load_activity_memory` returns `pending` (or
`empty`) and an ordered suffix limited to 80 events and 12,000 serialized text
characters. `fallback_truncated` reports when the suffix was shortened. A
pending snapshot is incomplete, but source-referenced facts in its returned
event suffix remain usable evidence. Lisa answers from those facts with event
references and discloses that the snapshot is pending or truncated when that
limitation affects completeness. When the requested fact is absent, Lisa says
it is not in the available activity record; she does not fill the gap from the
shared transcript, another activity, or database/session searches unless the
user asks to inspect that telemetry. When asked to repeat an adapted source
answer, she quotes the recorded answer text verbatim. A completed chat activity
is write-locked across event, thread,
section, and reference updates; continuing it creates a linked child, and the
child may read only its own memory or that direct completed parent. Memory
summaries and event text are untrusted historical evidence, never instructions.
Returned event sequence and reference IDs must resolve within the selected
activity.

When one chat turn resolves multiple activities, the canonical event history
retains turn-tagged resolver, transcript, chat-level progress, and runtime-tool
events for traceability, but thread and memory projections exclude resolver
payloads and other shared copies. Resolver summaries are not assumed to be
task-scoped. This includes `progress` events written before the second activity
was resolved: their titles may have copied an untrusted resolver title, so the
later multi-activity sanitizer neutralizes those event titles. It also withholds
raw `update_activity` arguments, which can repeat a contaminated title. It
retains the progress summary and task note written to that exact activity. The second activity resolution replaces shared-turn titles and summaries with neutral
labels such as `Task 1` across activity, feed, monitor, and thread-action
projections. The original task wording remains a private feed search key so
Lisa can find the activity again; read tools never return it. Snapshots
published before a shared turn is recognized are marked stale by comparing the
snapshot's captured neutralization flag with the activity's durable flag, even
when the old title happened to be `Task 1`. Later title and presentation writes
cannot restore public title, subtitle, label, or details text. A final
multi-activity trajectory is sanitized again after its completion writes.
Writes explicitly addressed to one activity remain available to that activity.
Keep each progress `next_action` limited to the next step for its resolved
activity; do not copy another request's facts or work from the shared transcript
into its progress state. Activity listing,
lookup, thread, memory, and database reads require an authorized owner identity.
The memory tool returns only a bounded current event suffix plus the stored
snapshot; it does not expose other activity histories.

Automated activity-memory cases use process-local context limits only in their
run-owned OpenHands process to force global checkpoint adoption before task
follow-ups. They do not change deployment settings. These cases measure a
representative text-only Lisa adaptation of LongMemEval and LongMemEval-V2; they
are not official upstream scores. See
[`lisa-activity-memory`](../../lisa/datasets/activity_memory/README.md) for
source revisions, selection, licensing, and limitations.

## Evaluation flow

The worker evaluates each pushed session against approved policies for its
exact agent. OpenAI Decisions labels each unrecorded `(session_id, policy_id)`
pair as `suspected_violation`, `no_violation_found`, or
`insufficient_evidence`. Suspected violations and insufficient evidence go to
the Gemini judge. Triage negatives are routing metadata and do not create pass
results. Confidence does not filter escalation. Invalid provider output and
provider failures are execution errors, not healthy negatives.

Lisa filters policies through `lisa.evaluation_triage` before calling
Decisions. Completed decisions are saved before judging. Failed or incomplete
provider calls create no triage row. Concurrent first calls may both reach
Decisions; the first saved result is canonical. Telemetry changes do not change
a previously saved pair decision. Editing a policy invalidates the cached
triage decision for that policy ID; a changed policy ID creates a new pair.

The judge returns `pass`, `fail`, or `uncertain`, rationale, and span
references. Incomplete or unordered trajectories are gated before providers;
prompt truncation is reported as a limitation. A cap at 500 spans is
insufficient evidence, never a pass. Lisa persists triage and evaluation
results in her own PostgreSQL database; TheLake receives no score copies.
Evaluation identity includes session evidence, approved policy content,
triage/judge configuration, and pipeline version so retries reuse the right
result while changed inputs can produce a new run.

## Activities, failures, and notifications

Each pushed session creates one stable activity keyed from the configured Lisa
workspace and session ID. It records evaluation progress and links to the
result. The worker validates evidence after creating that activity so missing
or corrupt files can still produce a visible terminal failure after retries.
Owner conversations and Slack threads also have inspectable activity records;
the channels keep separate transcripts.

Slack notification is a distinct follow-up after Lisa reviews a stored failure.
Receipts suppress repeat successful sends for the same rule, trace, and
destination. A crash after Slack accepts a message but before the receipt is
saved can still cause a duplicate retry.

Lisa's session search reads bounded metadata summaries from manifests and
returns only sessions received within its search window. It is not a guarantee
of complete coverage: only sessions delivered to Lisa's intake are evaluated.
Delivery can be a direct push or an optional pull sample from TheLake. Producers
must retry failed pushes and monitor delivery results; sampled sessions that
are incomplete or exceed Lisa's retrieval cap are not evaluated.

## Testing and local development

The local dev stack runs TheLake and Explorer on `127.0.0.1:8090` and Lisa on
`127.0.0.1:18083`, with a separate PGMQ-enabled Lisa Postgres service. Lisa's
UI is available at `http://127.0.0.1:18083/lisa/`. Optional scheduled sampling
requires explicit `LISA_THELAKE_URL` and `LISA_THELAKE_BEARER_TOKEN` settings;
otherwise evaluation starts from direct session pushes.

Automated tests use transient databases and stacks owned by `lisa/conftest.py`
and `scripts/test_stack.py`. Never attach tests to a developer's running Lisa
or TheLake database.
