# Policy memory and evaluation

Lisa keeps business rules in the `lisa.policies` PostgreSQL table. A workspace
rule applies across the workspace; an agent rule applies to one exact agent
name. Each rule has a stable string ID, criteria, approval state, and timestamps.
`list_policies` lists active rules across scopes. `read_policy` reads workspace
rules by default; pass the exact agent name with agent scope to read that
agent's rules. Its headings include the same paragraph-style ID shown in the
list. The tools present rules as readable Markdown, while Postgres remains the
only source of truth.

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
policy or approval. Lisa reads policy text only from Postgres; leftover files
from earlier setups are not imported or listed. Old evaluator records do not
grant approval to a policy.

An agent section overrides the workspace section of the same title, including
when the agent section is unapproved. `evaluate_session` resolves the effective
approved rules for the exact agent internally before selecting which ones apply.

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
with a rationale and captured span references. Incomplete or unordered trajectories
produce `insufficient_evidence`; a provider/validation error produces an
execution error. No numeric threshold or uncertainty margin is used.

TheLake stores each categorical score with the policy ID (for example, `P1.2`)
and the criteria text used for that judgment. Policies live in `lisa.policies`;
evaluations load the current approved rule and pass it to the judge.

Lisa investigates stored failures and can send configured Slack alerts. Pending
failure records preserve delivery destination across restart.

## Implementation

The standard MCP tools are exposed from
`lisa/src/lisa/controllers/mcp.py` and composed through
`bootstrap.py`. `services/policies.py` parses and validates rules;
`repositories/policies.py` persists records with a Postgres transaction and
compare-and-swap lock. `services/evaluations.py` loads an approved rule, passes
it to the judge, and stores the score. The tool package is independent of
OpenHands; the backend supplies trusted workspace credentials and
owner-confirmation context when launching it.
