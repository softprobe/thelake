# Lisa: the proactive agent QA engineer

**Status:** Product direction; parts are implemented, but the complete loop is not.

## Product promise

Every workspace has a persistent Softprobe agent named Lisa: an AI QA engineer
for the business and its production agents. Lisa learns how the business is
supposed to work, applies that knowledge to real agent sessions, and takes
responsibility for finding and explaining behavior problems. Customers should
not need to write evaluator code, label every failure first, or operate a
separate evaluation platform to get value.

The outcome is an actionable answer to: **Which agent behavior is putting a
customer, business outcome, or policy at risk; what evidence shows it; and what
should we do next?** Traces are evidence Lisa uses, not the product outcome by
themselves.

## Lisa's operating model

Lisa is one continuing agent identity per workspace, with an isolated VM
runner and a durable home directory. The runner gives Lisa a place to work
with approved tools and retain simple, inspectable notes. It can be started
when Lisa has work and stopped when idle; her identity, approved memory, and
conversation persist independently of a live VM process. She is not a separate
agent instance for every trace. Evaluation work can be parallel and bounded,
while Lisa owns the workspace-level context, follow-up, and conversation.

The workspace boundary applies to Lisa's conversation, files and memory,
credentials, source access, telemetry, evaluator configuration, and findings.
Lisa receives only the data and capabilities the workspace has authorized.
The specific VM provider and agent runtime are implementation choices; the
workspace-scoped identity, isolation, durable home, and tool boundary are the
product contract.

```text
Approved business sources ─┐
Agent definitions/tools ────┼─> Lisa's workspace knowledge ──┐
Production sessions ────────┘                                │
                                                             v
New session ─> capture and correlate ─> apply checks ─> evidence-backed issue
                                                       │
                                                       v
                                         Lisa follows up in chat
                                         and tracks resolution
```

## What Lisa learns

Lisa builds a practical model of the business from three evidence classes:

1. **Authoritative sources:** approved policy pages, support procedures,
   product documentation, and other sources the workspace connects or provides.
2. **Agent behavior and capabilities:** agent identity, prompts or instructions
   when explicitly made available, tool descriptions, and the order and results
   of tool calls in captured sessions.
3. **Operational evidence:** repeated session patterns, successful outcomes,
   failures, user feedback, and confirmed corrections.

Lisa keeps the knowledge understandable and reviewable. Start with plain
Markdown policy notes in Lisa's persistent home: one workspace-wide
`POLICY.md`, plus an optional `POLICY.md` for each registered agent. Each rule
should say what must happen, when it applies, what evidence can establish it,
and where it came from. Preserve a source reference and the date or revision
used when available. Keep the source content and rule text readable without a
proprietary memory viewer.

Lisa distinguishes:

- **Confirmed rules:** grounded in an authoritative source or owner
  confirmation. These may drive actionable QA checks.
- **Candidate rules:** plausible interpretations or emerging patterns that
  need confirmation. Lisa can ask about or present these, but must not alert as
  though they were established policy.
- **Observed behavior:** what agents have done, without claiming that it is
  required or correct.

An observed trace alone does not prove a business rule. A repeated behavior can
help Lisa notice a pattern and ask a useful question, but it does not silently
become policy. When sources conflict, Lisa records the conflict and seeks
resolution rather than choosing a rule without evidence. In the product, an
owner must confirm a rule before it becomes eligible to alert, and the
resulting evaluator must be explicitly activated. A model-generated status or
source citation alone is not approval.

Plain files are the initial memory interface, not a commitment to build a
general-purpose memory platform. Add a separate memory service only if concrete
needs such as concurrent writes, audit history, permissions, retrieval scale,
or cross-workspace controls cannot be met cleanly by files and existing
workspace storage.

## How Lisa finds issues

Lisa works continuously after an agent is connected. A user does not have to
first identify a bad session or ask Lisa to evaluate it.

For each newly captured session, the QA loop:

1. Resolves the workspace and agent, then selects the applicable confirmed
   rules and enabled checks.
2. Collects the bounded session evidence needed to assess those rules,
   including relevant tool calls, results, messages, and outcome context.
3. Runs checks through an evaluation runtime. Natural-language rubrics and
   existing evaluator frameworks are preferred over customer-authored code
   for ordinary business behavior checks.
4. Stores the verdict, rationale, evidence references, rule/evaluator version,
   and processing status so the result can be explained and reproduced.
5. Groups related failures into an issue Lisa can own, investigate, and discuss.

Lisa should report more than a score. An issue should state the affected agent
and behavior, the business rule and source, the session evidence, likely impact,
how certain the finding is, whether it is new or recurring, and a useful next
action. Where evidence is incomplete or the evaluator is uncertain, Lisa says
so and avoids presenting a guess as a confirmed defect.

“Near real time” means evaluation starts promptly after capture and produces a
finding while the session is still useful to an operator. It does not promise
synchronous evaluation in the agent's request path. Ingestion must remain
independent of a slow or unavailable judge. Before claiming continuous
monitoring, accepted evaluation work must be durably queued, idempotent, and
visible as pending, completed, or failed, with retries that cannot silently
erase QA work. An in-process best-effort trigger does not satisfy this bar.

## Human interaction

The default surface is a web chat for Lisa, backed by the same workspace
conversation and findings. A user can ask what Lisa learned, inspect or correct
a candidate rule, investigate an issue, and request a proposed fix. The web
experience should render evidence naturally: messages, tool activity, code,
diffs, and structured evaluation results.

Slack can be an optional channel for notifications and continued conversation.
The core workflow must not require a customer to be a Slack administrator or
install Slack. Chat is how people direct and understand Lisa; it is not a gate
before automatic monitoring runs.

Lisa may inspect an authorized agent repository or propose a code change only
when the workspace grants that access. The QA loop itself must work from
captured telemetry and approved business context without requiring repository
checkout or code changes.

## Product boundaries and safety

- Keep capture, evaluation, and Lisa's longer-running investigation as separate
  responsibilities. A judge can evaluate a session; Lisa owns workspace
  knowledge, issue follow-up, and user interaction.
- Treat telemetry, repository content, and retrieved documents as untrusted
  input. They cannot override system instructions or grant new permissions.
- A policy source citation and exact evaluator version must remain attached to
  a finding. Updating a policy must not rewrite the basis of a past verdict.
  Citations are author-provided context, not a verified proof of authority.
- Never use candidate or observed notes as confirmed rules for an alert.
- Keep secrets out of Markdown memory, chat history, evaluator prompts, and
  findings. Use the workspace's credential boundary and least privilege.
- Make evaluation cost, delay, failures, and coverage visible. Do not imply
  that an unevaluated session passed.
- Avoid storing duplicate telemetry or introducing a second trace pipeline;
  evaluate from the workspace's captured session evidence.

## Delivery sequence

The product direction is the complete ownership loop. Delivery can be staged
without weakening that goal:

1. **Connect and observe:** connect a supported agent using the Softprobe SDK
   or auto-instrumentation; identify sessions and agents reliably.
2. **Give Lisa a home:** run one isolated, persistent Lisa runner per
   workspace when needed; provide the approved tools and simple Markdown
   memory files in durable workspace storage.
3. **Learn with provenance:** gather authorized sources and session patterns;
   propose candidate rules, explain their evidence, and support user
   corrections and confirmation.
4. **Check sessions automatically:** apply confirmed rules to new sessions
   using natural-language evaluators and a reliable job path; create
   evidence-backed findings without a user-supplied incident.
5. **Own and resolve issues:** group recurrence, investigate with authorized
   context, converse in web chat (and optionally Slack), and propose
   remediation with human approval for consequential changes.

Each step must show user value on its own, but the success criterion is not
“rules were authored” or “traces were scored.” It is that Lisa catches a real,
meaningful agent behavior problem, explains it with evidence, and helps the
team resolve it.

## Current implementation versus direction

This page describes the product direction. The current public implementation
contains telemetry capture and query, chat-first evaluator authoring, an online
evaluation runner, and a first Markdown policy workflow with reusable learning
and review skills. That policy workflow is currently invoked explicitly; it is
not yet a persistent, automatically learning Lisa agent with a per-workspace VM,
continuous rule discovery, durable issue ownership, or complete retryable
evaluation job processing. Evaluator criteria can carry a short Markdown source
citation directly; no separate policy registry or policy API is required. The
product must capture owner confirmation and explicit evaluator activation
before a rule can alert. Agent targeting
currently uses names from telemetry rather than a first-class agent registry.
Do not describe these target capabilities as available until they are
implemented and verified end to end.

Related implementation details: [Runtime architecture](overview.md),
[Policy memory guide](../how-to/policy-memory.md), and
[Policy dogfood workflow](../how-to/policy-dogfood.md).
