# Lisa: standalone policy evaluation agent

## Ownership and deployment

Lisa is an independently deployable Python service. It serves its own web UI
and API, stores policies, activities, triage decisions, and evaluation results
in its own PostgreSQL database, and stores immutable session evidence in its
durable agent-workspace directory. Explorer remains a separate TheLake app.
Lisa has no runtime dependency on TheLake.

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
  tools. Lisa's developer skills do not become product skills unless the
  runtime loads them from `lisa/skills/`.

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
of complete coverage: only sessions pushed to Lisa are evaluated. Producers
must retry failed pushes and monitor delivery results.

## Testing and local development

The local dev stack runs TheLake and Explorer on `127.0.0.1:8090` and Lisa on
`127.0.0.1:18083`, with a separate PGMQ-enabled Lisa Postgres service. Lisa's
UI is available at `http://127.0.0.1:18083/lisa/`. The stack does not schedule
autonomous QA; evaluations start from pushed sessions.

Automated tests use transient databases and stacks owned by `lisa/conftest.py`
and `scripts/test_stack.py`. Never attach tests to a developer's running Lisa
or TheLake database.
