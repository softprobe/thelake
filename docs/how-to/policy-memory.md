# Policy memory and evaluation

Lisa keeps business rules in workspace-scoped `POLICY.md` files. A shared file
applies across the workspace; agent files apply to one exact agent name. These
files are the source of truth for policy QA.

```md
# Agent policy

## Check eligibility before issuing a refund
approved: yes

Check refund eligibility and explain the result before issuing a refund.
Wait for customer confirmation before submitting an eligible refund.
```

Put exactly one `approved: yes/no` line immediately below each rule's `##` heading.
Rules marked no, or missing approval, are not evaluated. Approval mentioned in
prose or code examples does not enable a rule. Invalid or duplicate approval
metadata is rejected. The approval line is excluded from the judge's criteria.

## Save and approve a rule

Ask Lisa to propose the exact rule for the intended workspace or agent. Review
its text and approval value, then send `Confirm policy rule: <exact heading>`.
Lisa uses `update_policy` to save that section. The same confirmation is required
for text edits, approval changes, and removal; the model cannot approve itself.
A saved rule with `approved: yes` is eligible immediately. There is no separate
behavior-check draft, evaluator registry, or activation step.

To pause future evaluation, ask Lisa to change the rule to `approved: no` and
confirm the named change. Scheduled Lisa can read and evaluate but cannot edit
policy or approval. Existing files are not rewritten on upgrade: headings without
approval remain visible but disabled until an owner explicitly approves them.
Old evaluator records do not grant approval to a Markdown rule.

An agent section overrides the workspace section of the same title, including
when the agent section is unapproved. Use `list_approved_policies` with the exact
agent name to see the effective approved rules.

## Execution and findings

Lisa's `search_sessions` tool uses the same date window, filters, cursor, and
summary response as TheLake's session-search API. It returns summaries only;
`get_session` loads one session and projects its spans into a compact sequence
of user messages, tool calls and results, and assistant replies. Evaluation uses
that same projection. Repeated system prompts, tool schemas, and model-usage
metadata are omitted, while tool arguments, results, status, timestamps, and
span references remain available to the judge.

Lisa's schedule searches recent sessions and applies the approved policy text
to those trajectories. The Gemini judge returns `pass`, `fail`, or `uncertain`
with a rationale and captured span references. Incomplete or unordered evidence
produces `insufficient_evidence`; a provider/validation error produces an
execution error. No numeric threshold or uncertainty margin is used.

TheLake stores each categorical score with the exact policy section and its
content hash. Repeated evaluation of the same policy content and trace reuses its
stored verdict. Editing the rule changes the hash and allows a fresh run. These
snapshots explain historical findings; they are not another policy registry.

Lisa investigates stored failures and can send configured Slack alerts. Pending
failure records preserve the policy snapshot and delivery destination across
restart. Historical scores and legacy pending failures remain readable; legacy
criteria are never loaded as current policy to start new evaluations.

## Implementation

The standard MCP tools are composed in `lisa-agent/src/thelake_lisa/mcp_tools.py`.
`policies.py` parses rules and protects owner-confirmed writes with a cross-process
compare-and-swap lock. `evaluation/service.py` captures one approved snapshot for
its lock, judge request, stored score, and recovery record. The tool package is
independent of OpenHands; the backend supplies trusted workspace credentials and
owner-confirmation context when launching it.
