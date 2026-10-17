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

Lisa proposes the exact section and waits for the owner's standalone command
`Confirm policy rule: <exact heading>` before saving a change, including changing
approval. This trusted context is supplied by the chat controller, not a model
argument. Saving with `approved: yes` is the only approval step needed for QA.
There is no separate evaluator definition, draft registry, version, activation,
or per-check score threshold. Future optional settings belong with the rule.

Traces show behavior, not business intent. Lisa never silently converts observed
behavior into approved policy. Unapproved proposals can be discussed in reports,
but cannot drive policy verdicts. Scheduled turns cannot write policy.

## Evaluation terms

Lisa uses three distinct names for evaluation inputs and outputs:

| Term | Meaning |
|------|---------|
| **session detail** | Raw `GET /v1/sessions/{id}` payload from TheLake (spans, scores, agent names, truncation flags). |
| **trajectory** | Compact ordered user/tool/assistant events plus completeness flags, built from session detail for policy selection and judging. |
| **references** | Span citations on a judge verdict. Not the trajectory. |

Do not call the trajectory “evidence.” “Evidence” in prose means telemetry or
supporting facts in general; code uses the terms above.

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
- `services/evaluations.py` loads an approved policy and coordinates selection,
  judging, idempotence, and result storage.
- `repositories/failures.py` and `services/failures.py` own interrupted
  score-write recovery and follow-ups.
- `repositories/notifications.py` and `services/notifications.py` own notification
  receipts. `slack.py` shares transport between alerts and conversation replies.
- `storage.py` supplies cross-process locks and durable atomic file replacement.

## Scheduled review

One process-local APScheduler job, `Lisa scheduled QA`, runs a five-field UTC cron
from `LISA_QA_CRON`, default `0 * * * *`. It does not run on startup. The quickstart
uses `*/5 * * * *`. Each firing starts a fresh model turn using `own-agent-qa`.
Run one scheduler process per workspace; multiple replicas can each fire a turn.

Lisa first resumes pending failures, then inspects recent sessions and policy
memory. For each agent she checks recent unchecked sessions against approved
policies. OpenAI Decisions gives each policy a binary applicability choice and
confidence; Lisa judges only `applicable` choices at or above
`LISA_POLICY_SELECTION_MIN_CONFIDENCE` (default `0.8`). If none clear that bar,
selection returns an empty selected set (below-cutoff answers are dropped, not
audited separately). She prioritizes consequential business risks, tool
errors, incomplete outcomes, and unusual repeated behavior. Ordinary discovery
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

TheLake stores the categorical score with the policy ID, criteria text, policy
hash, span references, and limitations. Identical policy content and trace reuse
an existing non-error verdict; a criteria edit produces a new hash and permits a
fresh run.

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
