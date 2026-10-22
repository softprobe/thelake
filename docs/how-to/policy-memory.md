# Policy memory and evaluation

Use the shared [Lisa evaluation terms](../architecture/lisa-terms.md): a
trajectory is derived from a session and has no separate ID; policy pairs use
`(session_id, policy_id)`.

Lisa keeps business rules in the `lisa.policies` PostgreSQL table. A workspace
rule applies across the workspace; an agent rule applies to one exact agent
name. Each rule has a stable string ID, criteria, approval state, and timestamps.
`list_policies` lists active rules across scopes. `read_policy` reads workspace
rules by default; pass the exact agent name with agent scope to read that
agent's rules. Its headings include the same readable ID shown in the
list. The tools present rules as readable Markdown, while Postgres remains the
only source of truth.

```md
# Agent policy

## Check eligibility before issuing a refund
approved: yes

Check refund eligibility and explain the result before issuing a refund.
Wait for customer confirmation before submitting an eligible refund.
```

The `approved: yes/no` line displays the policy row's `approved` column; it is
not part of stored criteria. Lisa's write tool accepts a name and criteria, and
the owner approves a pending row by its readable ID. Mentioning approval in
criteria or code examples does not enable a policy. Only rows whose `approved`
column is true are evaluated.

## Create, update, and approve a rule

Ask Lisa to create a rule in workspace or exact agent scope. Lisa writes an
unapproved row to `lisa.policies` and returns its readable ID (`P1.x`). Each
ID is one policy section; for example, `P1.1` and `P1.1.1` are independent
policies. To change a rule, Lisa must identify and pass that rule's exact ID to
`write_policy`. The tool updates that same database row, preserving its ID and
creation time. It must not create a second row for the changed policy. If the
row was approved, changing its name or criteria sets `approved: no` on that row
until its owner approves the same ID again. `list_policies` and `read_policy`
show the current rows and approval state.

Approve the exact policy by sending `Approve policy: P1.x` as a standalone
message. The authenticated owner check and exact-ID check are enforced by the
runtime. Lisa sets that same row's `approved` value to true. Scheduled Lisa can read
and evaluate but cannot create, update, approve, or delete policy. Policy rows and
evaluation history live in Postgres. Updating a policy invalidates its cached
triage decisions; saved evaluation results retain the criteria snapshot used
when they were produced.

To delete a policy, an authorized owner can ask Lisa to delete it by ID. Lisa
checks the exact current ID with `list_policies` or `read_policy`, then removes
the policy row and cached triage decisions. Historical evaluation rows retain
the policy ID, name, and criteria hash after deletion. Scheduled QA cannot
delete policies. Substantive policy work is also recorded in Lisa's
activity workflow; the activity captures chat actions while the policy row
remains the source of truth for policy state.

Only an approved agent rule overrides an approved workspace rule with the same
name.
`evaluate_session` resolves the approved candidate set for the exact agent
internally; it does not accept a caller-supplied policy list.

New policy IDs are allocated by reading the greatest numeric ID in
`lisa.policies` while holding a transaction advisory lock shared across policy
scopes. This keeps IDs unique across workspace and agent rules.

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
does not create a pass score. Lisa saves each completed pair decision in its
durable triage cache and records the pairs used by an action in an inspectable
activity. Incomplete or unordered trajectories are gated as
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
`bootstrap.py`. `services/policies.py` validates owner writes and exact-ID
approval; `repositories/policies.py` persists records with a Postgres
transaction and scope lock. Benchmark catalog seeding lives separately under
`lisa/benchmark`. `services/evaluations.py` resolves approved candidates,
calls `evaluation/policy_triage.py`, judges escalations, and stores scores. The
tool package is independent of OpenHands; the backend supplies trusted
workspace credentials and owner-confirmation context when launching it.
