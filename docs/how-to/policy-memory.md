# Policy memory and evaluation

Use the shared [Lisa evaluation terms](../architecture/lisa-terms.md): a
trajectory is derived from a session and has no separate ID; policy pairs use
`(session_id, policy_id)`.

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
approved candidate set for the exact agent internally. It does not accept a
caller-supplied policy list.

## Execution and findings

Lisa's `search_sessions` tool uses the same date window, filters, cursor, and
summary response as TheLake's session-search API. It returns summaries only;
`get_session` loads one session and projects its spans into a compact sequence
of user messages, tool calls and results, and assistant replies. Evaluation uses
that same projection. Repeated system prompts, tool schemas, and model-usage
metadata are omitted, while tool arguments, results, status, timestamps, and
span references remain available to the judge.

For each eligible session, Lisa builds the production trajectory and sends one
Decisions question for every approved candidate policy read from PostgreSQL.
Decisions returns `suspected_violation`, `no_violation_found`, or
`insufficient_evidence` per policy. Lisa escalates suspected violations and
insufficient evidence to the Gemini judge, which returns `pass`, `fail`, or
`uncertain` with a rationale and captured span references. A triage negative
does not create a pass score. Incomplete or unordered trajectories are gated as
`insufficient_evidence` before provider calls; provider or validation errors
remain execution errors. No numeric threshold or uncertainty margin is used.

TheLake stores each categorical score with the policy ID (for example, `P1.2`)
and the criteria text used for that judgment. Policies live in `lisa.policies`;
evaluation gets candidates through `PolicyService`, records triage decisions,
and passes only escalated approved rules to the judge.

Lisa investigates stored failures and can send configured Slack alerts. Pending
failure records preserve delivery destination across restart.

## Implementation

The standard MCP tools are exposed from
`lisa/src/lisa/controllers/mcp.py` and composed through
`bootstrap.py`. `services/policies.py` parses and validates rules;
`repositories/policies.py` persists records with a Postgres transaction and
compare-and-swap lock. `services/evaluations.py` resolves approved candidates,
calls `evaluation/policy_triage.py`, judges escalations, and stores scores. The
tool package is independent of OpenHands; the backend supplies trusted
workspace credentials and owner-confirmation context when launching it.
