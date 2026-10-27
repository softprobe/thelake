# Policy memory and evaluation

Use the shared [Lisa evaluation terms](../architecture/lisa-terms.md): a
trajectory is derived from a session and has no separate ID; policy pairs use
`(session_id, policy_id)`.

Lisa keeps business rules in the `lisa.policies` PostgreSQL table. Its columns
are `id`, `scope`, `target_agent_name`, `name`, `criteria`, `approved`,
`created_at`, and `updated_at`; it has no `archived_at` field. Deleting a policy
removes its row. A workspace rule applies across the workspace; an agent rule
applies to one exact agent name. Each rule has a stable string ID, criteria,
approval state, and timestamps.
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

An authorized owner approves a pending row by asking Lisa to approve its
readable ID. Lisa calls `approve_policy` with that ID and sets the same row's
`approved` value to true. Pushed sessions are evaluated by the service
worker. That worker is not an owner conversation and cannot create, update,
approve, or delete policy. Policy rows and
evaluation history live in Postgres. Updating a policy invalidates its cached
triage decisions; saved evaluation results retain the criteria snapshot used
when they were produced.

To delete a policy, an authorized owner can ask Lisa to delete it by ID. Lisa
checks the exact current ID with `list_policies` or `read_policy`, then removes
the policy row and cached triage decisions. Historical evaluation rows retain
the policy ID, name, and criteria hash after deletion. The session evaluation
worker cannot delete policies. Substantive policy work is also recorded in Lisa's
activity workflow; the activity captures chat actions while the policy row
remains the source of truth for policy state.

Only an approved agent rule overrides an approved workspace rule with the same
name.
`evaluate_session` resolves the approved candidate set for the exact agent
internally; it does not accept a caller-supplied policy list.

New policy IDs are allocated by reading the greatest numeric ID in
`lisa.policies` while holding a transaction advisory lock shared across policy
scopes. This keeps IDs unique across workspace and agent rules.

## Policy discovery and owner review

Pushed-session policy-gap discovery is a separate capability from triage and
judgment. By default it reserves at most five sessions per workspace per hour
for review; deployments can change this with
`LISA_POLICY_DISCOVERY_SAMPLES_PER_HOUR` (valid range 1–100). Where a selected session already reaches the Gemini judge, an
optional gap signal can be returned alongside the verdict; otherwise the
discovery worker invokes the configured gap analyzer. The signal never changes
the verdict. When Lisa detects a clear error in one sampled run, she reacts
immediately in the main chat with the cited evidence from that run. A reusable
policy recommendation is held until at least three distinct sampled sessions
show the same kind of error, and it must cite three distinct sessions.
Replaceable analyzer and synthesizer callables keep model choice separate from
sampling, persistence, and review contracts.

Lisa stores cited signals and suggestions in dedicated Postgres tables, not in
`lisa.policies` or rebuildable activity metadata. A scheduled consolidation
compares gathered signals with approved policy and prior owner decisions. The
main Lisa chat shows pending recommendations with their rationale, scope,
supporting-session count, and cited trace evidence. The Policy suggestions feed
remains the full review view; both it and the chat card provide owner review
actions. Policy MCP tools expose evidence and source session context. Chat may
discuss a suggestion, but only an authenticated owner action from a chat card,
the feed, or an owner-authorized policy tool creates an active rule.
Approval, edits, and dismissal are retained as discovery decisions so later
consolidations can avoid repeating rejected or already adopted suggestions.
The chat's durable proactive-update table stores references to immediate
sampled-run QA observations, suggestions, and completed pushed-session
findings. The source model call supplies each update's user-facing message:
the gap analyzer writes the immediate observation, the synthesizer writes the
recommendation rationale, and the judge writes a later finding. Explorer renders
those messages as supplied and does not build Lisa's words from trace fields.
The projection is not a transcript turn or activity event. The
bounded owner-only update endpoint returns stable source references; the
Explorer loads suggestions and findings through the existing suggestion and
activity endpoints, and sampled observations through the owner-only
`GET /v1/qa-observations/{id}` endpoint. The same suggestion card remains
visible after approval with its active policy ID, while dismissed suggestions
stay in review history and leave the chat timeline. A finding appears only
after a pushed-session evaluation completes successfully with a failed policy
verdict; missing, failed, incomplete, and passing evaluations do not create a
finding update. Questions from these cards still go through Lisa's normal chat
runtime.
Every successful daily consolidation records its whole input batch as reviewed,
including signals that produced no suggestion, so old evidence is not submitted
again and newer signals are not starved. Suggestion list and evidence reads are
owner-only because cited spans may contain user and tool data.
The worker makes at most one consolidation attempt per day when signals exist;
provider failures leave signals pending for the next day's attempt.

The labeled policy-discovery evaluation under
`lisa/datasets/policy_discovery/` covers trace-gap detection and proposal
consolidation. Its consolidation cases measure exact and semantic duplicate
suppression, conflict suppression, compatible-overlap reporting, repeated
signal consolidation, and prior owner approvals or dismissals. The set is a
small synthetic diagnostic benchmark. Positive proposals are also checked for
expected scope, target agent, policy behavior concepts, cited signal coverage,
and overlap relation. See its README for the live evaluation command and
cost-bounded call count; these cases are not a production-quality estimate.

## Execution and findings

Lisa's `search_sessions` tool searches the session bundles pushed to Lisa's
local trace store. Its default time window is the last seven days; it supports
filters for agent, user, model, errors, root traces, bounded result count, and
ordering. It does not support cursor pagination. `get_session` loads one local
bundle, applies evidence redaction, and bounds the span list. Lisa then projects
the spans into the ordered trajectory used by both policy triage and judging.
Repeated system prompts, tool schemas, and model-usage metadata are omitted,
while tool arguments, results, status, timestamps, and span references remain
available as evidence. If ordering or evidence is incomplete or truncated, the
trajectory carries limitations and Lisa gates it as insufficient evidence.

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

Lisa stores each categorical score with the policy ID (for example, `P1.2`)
and the criteria text used for that judgment in Lisa Postgres. Policies live in
`lisa.policies`; evaluation gets candidates through `PolicyService`, records
triage decisions, and passes only escalated approved rules to the judge.

Lisa investigates stored failures and can send configured Slack alerts. Pending
failure records preserve delivery destination across restart.

## Implementation

The standard MCP tools are exposed from
`lisa/src/lisa/controllers/mcp.py` and composed through
`bootstrap.py`. `services/policies.py` validates owner writes, approvals, and
deletions for a web or Slack turn. The policy ID is the `approve_policy`
argument. `repositories/policies.py` persists records with a Postgres
transaction and scope lock. Benchmark catalog seeding lives separately under
`lisa/benchmark`. `services/evaluations.py` resolves approved candidates,
calls `evaluation/policy_triage.py`, judges escalations, and stores scores. The
tool package is independent of OpenHands; the backend supplies trusted
workspace credentials when launching it.

The read-only `lisa_db` surface is defined by the ordered `RELATION_COLUMNS`
contract in `lisa/src/lisa/repositories/lisa_database.py`. The query validator
and agent schema resource use that same contract. Tests compare every listed
view projection against `lisa/src/lisa/sql/activities/schema.sql` and compare
the exact reader-role grants, including the restricted policy-column grant,
so a database change that isn't reflected in the tool contract fails before
it can ship. The
`readable_deployment` grant is only used to bind the reader connection and is
not available to user-authored queries.
