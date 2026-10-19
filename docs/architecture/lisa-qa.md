# Lisa: the proactive agent QA engineer

## Ownership contract

Lisa owns QA decisions, scheduling, evaluation, investigation, and Slack delivery.
TheLake stores authenticated workspace telemetry and resulting scores/findings.
Trace ingestion makes evidence available; it does not start Lisa or invoke a judge.
A user conversation or Lisa's own schedule starts work.

Lisa is one continuing agent identity per workspace. Her conversation, policy
records, credentials, telemetry access, and pending follow-ups are isolated by
workspace. The local Compose stack runs one workspace and persists Lisa's home
in the `lisa-data` volume. Multi-workspace deployments run scoped Lisa runtimes.

## Business policy is stored in Postgres

The `lisa.policies` table is the single source of truth. Each record has a
stable string ID, scope, target agent, name, criteria, approval, and timestamps.
Workspace rules apply across the workspace; an agent's own rule overrides a
shared rule of the same name. Lisa's typed tools present and accept readable
Markdown. The database is the only policy source; files from earlier setups
are not read or listed.

```md
## Check eligibility before issuing a refund
approved: yes

Check eligibility and explain the result before issuing a refund.
Wait for customer confirmation before submitting an eligible refund.
```

Each section has a human-readable title, one `approved: yes/no` line immediately
below the heading, and the requirements/source context. Missing approval means no.
An unapproved agent rule also suppresses an approved workspace rule of the same
name. Approval examples inside prose or code fences do not enable a rule.

Lisa stores the exact proposed section, expected current content, scope, and
activity link as a pending policy proposal. The owner confirms it with the
standalone command `Confirm policy proposal: <proposal_id>` in the main chat or
a Slack thread. This trusted ID is supplied by the chat controller, not a model
argument. Lisa applies the proposal only if the stored current policy still
matches. Saving with `approved: yes` is the only approval step needed for QA.
There is no separate evaluator definition, draft registry, version, activation,
or per-check score threshold. Future optional settings belong with the rule.

Traces show behavior, not business intent. Lisa never silently converts observed
behavior into approved policy. Unapproved proposals can be discussed in reports,
but cannot drive policy verdicts. Scheduled turns cannot write policy.

## Evaluation terms

Use the shared [Lisa evaluation terms](lisa-terms.md): `session_id` identifies
the evaluation input; a trajectory is the ordered evidence projection from that
session and has no separate ID. Span citations are references, not trajectories.
In prose, “evidence” means telemetry or supporting facts in general.

## Runtime and module boundaries

The OpenHands adapter loads standard AgentSkills via `AgentContext` and launches
the FastMCP package via `mcp_config`. The package does not import OpenHands and
can be launched by another backend with the same trusted workspace context.

- `controllers/mcp.py` exposes the standard tools; `bootstrap.py` composes
  workspace services for HTTP and MCP.
- `context.py` captures immutable workspace credentials and owner confirmation.
- `services/policies.py` and `models/policies.py` parse and validate rules;
  `repositories/policies.py` applies scope precedence and performs transactional
  compare-and-swap writes in Postgres.
- `thelake_client.py` signs workspace-scoped calls to TheLake's standard session
  and score APIs, and redacts errors.
- `evaluation/trajectory.py` builds a compact trajectory from session spans and
  reports truncation instead of pretending partial data is complete.
- `evaluation/evaluator.py` validates a trajectory and asks Gemini for a
  categorical verdict, rationale, and span references.
- `evaluation/policy_triage.py` asks OpenAI Decisions to triage approved
  policies for possible violations; its decisions only route candidates.
- `services/evaluations.py` loads approved policies and coordinates triage,
  judging, idempotence, and result storage.
- `repositories/failures.py` and `services/failures.py` own interrupted
  score-write recovery and follow-ups.
- `repositories/notifications.py` and `services/notifications.py` own notification
  receipts. `slack.py` shares transport between alerts and conversation replies.
- `storage.py` supplies cross-process locks and durable atomic file replacement.

## Scheduled review

One process-local APScheduler job, `Lisa scheduled QA`, runs a five-field UTC cron
from `LISA_QA_CRON`, default `0 * * * *` (hourly at minute 0). It does not run on
startup. The quickstart uses this hourly default; the dogfood E2E can temporarily
override it to `*/5 * * * *` to keep its wait within the test timeout. Each firing
starts a fresh model turn using `own-agent-qa`.
Run one scheduler process per workspace; multiple replicas can each fire a turn.

Lisa first resumes pending failures, then inspects recent sessions and policy
memory. For each agent she checks recent unchecked sessions against approved
policies. OpenAI Decisions triages each unrecorded session-policy pair as
`suspected_violation`, `no_violation_found`, or `insufficient_evidence`. One
inspectable Lisa activity records the action timeline and links to any judge
runs it starts.
Lisa sends suspected violations
and insufficient-evidence decisions to the Gemini judge; only that judge produces
final verdicts. A triage negative does not produce a passing score. Confidence
never filters escalation. All per-policy decisions are retained in selection
metadata and in Postgres under the `(session_id, policy_id)` pair key. Previously
triaged pairs are filtered from later Decisions requests. Telemetry changes,
policy edits, and triage configuration changes do not trigger another triage
for the same pair. If the triage prompt clips any evidence, all current candidates escalate as
insufficient evidence. Incomplete session capture or uncertain event ordering
still returns unresolved coverage before triage; this change does not add
context retrieval or investigation tools to Gemini. She prioritizes
consequential business risks, tool errors, incomplete outcomes, and unusual
repeated behavior. Ordinary discovery
can suggest new rules, but cannot establish approval. The previous bounded report
helps avoid repeating unchanged proposals every cycle.

The current schedule is a bounded sampling cap, not continuous lossless coverage.
TheLake's recent session list is bounded to seven days and 25 sessions. Lisa
fetches session detail for selected sessions, caps the returned span list at
500, and retains complete session totals and score history. A capped session is
reported as incomplete for evaluation.
Lisa evaluates the captured session once, associates its score with its first
trace ID, and does not guarantee independent evaluation of every trace in a
multi-trace session. She must describe those limits and never call unevaluated
sessions passes.

## Verdicts and historical scores

For a selected agent/rule/session, Lisa loads the current approved rule from
`lisa.policies`. Its scope, name, target agent, criteria, and derived hash
identify the evaluation lock and score. The local judge receives the live policy
text without approval metadata and a redacted trajectory. It returns `pass`,
`fail`, or `uncertain` with a rationale and span references. Incomplete/unordered
trajectories yield `insufficient_evidence`. Invalid provider output or provider
failures are execution errors and do not occupy the verdict ledger; a later run
can retry.

Lisa's tracked run identity includes the session, effective policy hash,
canonical trajectory digest, Decisions model, judge model, and evaluation
pipeline version. Postgres uses that identity to deduplicate completed work and
resume pending score writes. The stable score ID comes from the same identity;
TheLake scores are reporting copies, not Lisa's deduplication authority. Changed
evidence, policy criteria, model, or pipeline version creates a new run. Runs
created before this identity was introduced cannot be compared reliably and may
be evaluated once more during the transition.

The score in TheLake keeps the policy ID, criteria text, policy hash, trajectory
digest, span references, and limitations. Lisa stores every completed triage
decision in the pair-only `lisa.evaluation_triage` cache, including negative
decisions that produce no score. Lisa's generic activity event timeline records
the decisions and outcomes from that action. Evaluation runs link to activities
through `activity_id`; the triage cache remains independent of activity identity.

## Provider prompts and Lisa call records

The formal tool-event grammar, status rules, and truncation contract are in
[Lisa judge trajectory format](../reference/lisa-trajectory-format.md).

Policy triage and judging share a compact plaintext trajectory view. Each
judge event uses a short local citation (`e1`, `e2`, ...) and short tool-call
link (`c1`, `c2`, ...) where needed. Judge requests do not repeat full telemetry
IDs. Policy routing retains its existing ID labels. Recording metadata retains
local-to-real ID maps; judge references are validated against the displayed IDs
and resolved back to actual telemetry spans. For example:

```text
[e4] tool call: get_booking {"booking_id":"BK-1000"}; [e5] result: {"booking_id":"BK-1000","status":"confirmed"}
```

Paired calls and results share one line while keeping their source-span
citations. Failed calls use an `error,` clause. Exceptional tool names are
quoted. Message roles and tool associations are preserved.
Structured payloads use compact JSON; text and header control characters are
escaped so each event stays on one physical line. Both providers receive
redacted trajectory and policy text.

Only this provider view clips event bodies to 2,000 serialized characters.
Truncation markers report omitted characters, and limitations flow through
selection results (including empty selections), saved context, verdicts, and
score metadata. Stored trajectories retain their existing fidelity and bounds.
Upstream incomplete or unordered captures still cannot be judged. Prompt-view
clipping deliberately permits a verdict or policy exclusion from visible text;
omitted text can change either conclusion. The judge must return uncertain
when the visible text cannot support a conclusion. Recalibrate representative
policy fixtures after changing this prompt view.

Set `LISA_LLM_TRACE_ENABLED=1` to record Lisa's own provider calls separately
from target-agent trajectories. Under Lisa's workspace home,
`llm-calls/<call-id>.jsonl` contains a request line followed by a response or
sanitized error line. The request includes the actual application prompt,
model, available tool definitions/schema, formatting-loss metadata, and known
conversation/session/run/activity links. Triage records do not require a
selected policy or activity. Chat records capture LiteLLM-hook-observable
attempts, including SDK retries; they are not a wire-level capture. The
pair-only cache is independent of activities. The activity event timeline
records which cached or fresh decision each action used.

Recording is off by default. Before any write, records remove credential values
and provider thinking/reasoning content. Numeric token-usage counters such as `usage.prompt_token_count` and
`usage.total_token_count` remain numeric; unavailable counters remain `null`. Judge responses store parsed output once, falling back to visible raw
text only when parsing is unavailable. Error records retain exception types
without raw messages or tracebacks. Request files are written atomically before
calls; terminal writes replace them atomically. A crash may leave a readable
request-only file. Recording errors warn without interrupting Lisa. Directories
and files use owner-only permissions. Startup and hourly cleanup remove only
recorder-owned files older than 30 days, including abandoned requests. Existing
OpenHands runtime state and evaluation inputs are retained independently.

## Investigation and recovery

A failure is recorded in Lisa's workspace home with its score payload before the
remote score write. If that write is interrupted, the next follow-up reconciles
against stored scores and finishes it without invoking the judge again. Once the
score is acknowledged, the pending item is ready for investigation and reporting.
Historical scores and pending failures remain usable across recovery retries.

Slack notification is a separate action after Lisa reviews a stored failure.
Lisa verifies the queued trace and failing score, then uses the captured delivery
destination. Receipts suppress repeat successful sends for the same rule,
trace, and destination. A crash after Slack accepts a message but before its
receipt is persisted can still cause a duplicate retry. Lisa clears follow-up
work only after reporting and resolving notification delivery (or confirming no
destination). Removing the persistent volume removes this local recovery memory.

Durable scheduling, accepted-work queues, grouped issues, and visible coverage
status remain future requirements before claiming continuous monitoring.
