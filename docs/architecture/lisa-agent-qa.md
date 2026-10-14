# Lisa: the proactive agent QA engineer

## Ownership contract

Lisa owns QA decisions, scheduling, evaluation, investigation, and Slack delivery.
TheLake stores authenticated workspace telemetry and resulting scores/findings.
Trace ingestion makes evidence available; it does not start Lisa or invoke a judge.
A user conversation or Lisa's own schedule starts work.

Lisa is one continuing agent identity per workspace. Her conversation, policy
files, credentials, telemetry access, and pending follow-ups are isolated by
workspace. The local Compose stack runs one workspace and persists Lisa's home
in the `lisa-data` volume. Multi-workspace deployments run scoped Lisa runtimes.

## Business policy is plain Markdown

`POLICY.md` is the single source of truth. Workspace rules apply to agents across
the workspace; an agent's own rule overrides a shared rule of the same name.

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

## Runtime and module boundaries

The OpenHands adapter loads standard AgentSkills via `AgentContext` and launches
the FastMCP package via `mcp_config`. The package does not import OpenHands and
can be launched by another backend with the same trusted workspace context.

- `mcp_tools.py` exposes the standard tools; `services.py` composes their services.
- `context.py` captures immutable workspace credentials and owner confirmation.
- `policies.py` parses approved rules, applies scope precedence, and owns safe
  compare-and-swap policy writes.
- `thelake_client.py` signs workspace-scoped calls to TheLake's standard session
  and score APIs, and redacts errors.
- `evaluation/evidence.py` converts spans into ordered events and reports local
  truncation instead of pretending partial evidence is complete.
- `evaluation/evaluator.py` validates evidence and asks Gemini for a categorical
  verdict, rationale, and actual captured span references.
- `evaluation/service.py` captures one approved policy snapshot and coordinates
  judging, idempotence, and result storage.
- `evaluation/failures.py` owns interrupted score-write recovery and follow-ups.
- `notifications.py` owns notification receipts. `slack.py` shares transport
  between alerts and conversation replies.
- `storage.py` supplies cross-process locks and durable atomic file replacement.

## Scheduled review

One process-local APScheduler job, `Lisa scheduled QA`, runs a five-field UTC cron
from `LISA_QA_CRON`, default `0 * * * *`. It does not run on startup. The quickstart
uses `*/5 * * * *`. Each firing starts a fresh model turn using `own-agent-qa`.
Run one scheduler process per workspace; multiple replicas can each fire a turn.

Lisa first resumes pending failures, then inspects recent sessions and policy
memory. For each agent she lists applicable approved policies and checks recent
unchecked matching evidence. She prioritizes consequential business risks, tool
errors, incomplete outcomes, and unusual repeated behavior. Ordinary discovery
can suggest new rules, but cannot establish approval. The previous bounded report
helps avoid repeating unchanged proposals every cycle.

The current schedule is a bounded sampling cap, not continuous lossless coverage.
TheLake's recent session list is bounded to seven days and 25 sessions. Lisa
fetches standard session detail for selected sessions, caps the returned span
list at 500, and retains complete session totals and score history. A capped
session is reported as incomplete for evaluation.
Lisa evaluates the captured session once, associates its score with its first
trace ID, and does not guarantee independent evaluation of every trace in a
multi-trace session. She must describe those limits and never call unevaluated
sessions passes.

## Verdicts and historical evidence

For a selected agent/rule/session, Lisa captures the approved section once. Its
scope, name, target agent, and content hash identify the evaluation lock and score.
Edits during execution cannot change that run's criteria or historical snapshot.
The local judge receives the policy text without approval metadata and redacted
trace evidence. It returns `pass`, `fail`, or `uncertain` with a rationale and span
references. Incomplete/unordered evidence yields `insufficient_evidence`.
Invalid provider output or provider failures are execution errors and do not
occupy the verdict ledger; a later run can retry.

TheLake stores the categorical score with the exact policy section, content hash,
evidence references, and limitations. It needs no evaluator registry to interpret
the result. Identical policy content and trace reuse an existing non-error verdict;
a policy edit permits a fresh run. Historical snapshots never replace the live
policy file as configuration.

## Investigation and recovery

A failure is recorded in Lisa's workspace home with its score payload before the
remote score write. If that write is interrupted, the next follow-up reconciles
against stored scores and finishes it without invoking the judge again. Once the
score is acknowledged, the pending item is ready for investigation and reporting.
Historical scores and legacy pending failures remain usable after migration.
Registry definitions never automatically enable current Markdown rules.

Slack notification is a separate action after Lisa reviews a stored failure.
Lisa verifies the queued trace and failing score, then uses the captured delivery
destination. Receipts suppress repeat successful sends for the same rule snapshot,
trace, and destination. A crash after Slack accepts a message but before its
receipt is persisted can still cause a duplicate retry. Lisa clears follow-up
work only after reporting and resolving notification delivery (or confirming no
destination). Removing the persistent volume removes this local recovery memory.

Durable scheduling, accepted-work queues, grouped issues, and visible coverage
status remain future requirements before claiming continuous monitoring.
