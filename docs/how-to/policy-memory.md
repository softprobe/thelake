# Policy memory and evaluation tools

Lisa keeps business context as short Markdown in a workspace-scoped persistent
home. The shared file is `POLICY.md`; agent-specific files are stored below a
hashed exact agent name. `list_policies`, `read_policy`, and `update_policy`
are standard MCP tools. The model never supplies a filesystem path, and the
runtime checks workspace scope, path containment, compare-before-write, and
secret redaction.

Lisa’s workflows are defined by standard AgentSkills in the root
[`skills/`](../../skills/) directory. `POLICY.md` stores owner-approved rules as
named Markdown sections, without IDs, status fields, or evaluator metadata.
Lisa keeps proposed rules in chat and writes them only after the owner sends
`Confirm policy rule: <exact heading>`. The `review-agent-policy` skill checks
for stale, duplicate, conflicting, and unsupported rules. Lisa does not yet
fetch documents from external business systems. Lisa creates or reuses a
named evaluation after policy approval; an owner reviews and activates it in
Explorer. Scheduled Lisa reports policy coverage gaps but cannot create or
activate evaluations.

The tools live in `lisa-agent/src/thelake_lisa/mcp_tools.py` and use FastMCP.
They do not import OpenHands. OpenHands is one adapter: it loads the standard
skills with `AgentContext` and launches the tool package using its normal
`mcp_config` stdio support. The same MCP tool package can be launched by
another runtime by launching the same local stdio MCP package with workspace
identity and service credentials supplied by that runtime. The MCP process is
owned by the agent adapter; it is not a separately deployed service.

## Proactive findings

On a scheduled cycle, Lisa selects an unchecked session, applies its active
evaluations, and starts a finding conversation when an evaluation fails. She follows the
[`investigate-evaluator-failure`](../../skills/investigate-evaluator-failure/SKILL.md)
skill. She checks the relevant session and policy, then reports the evidence,
likely business impact, uncertainty, and a practical next step in Explorer.
If the evaluation has a Slack thread binding, Lisa also notifies that thread.

The score and evaluation version are durable in TheLake. Lisa keeps the
scheduled work in her workspace conversation and uses stable score IDs so a
repeated cycle does not create a second score for the same evaluation and trace.
