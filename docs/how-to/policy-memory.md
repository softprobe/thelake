# Policy memory for agent behavior checks

TheLake evaluators can be authored from short Markdown policies stored in the
workspace. A coding agent can read local instructions and authorized business
sources, propose policy edits, and turn confirmed requirements into a
versioned TheLake evaluator draft. TheLake stores policy documents and
evaluator versions in the authenticated workspace; it does not clone or read
the agent's repository.

This keeps policy authoring in ordinary language and avoids a second evaluator
codebase. The coding agent has local repository access; TheLake stores the
workspace and per-agent Markdown documents plus the composed rubric and source
digests. Online evaluation continues to use the existing evaluator runner.

## Add the skills to your coding agent

Use the reusable instructions in:

- [`learn-agent-policy`](../../skills/learn-agent-policy/SKILL.md) to learn or
  update policy memory and submit an inactive evaluator draft.
- [`review-agent-policy`](../../skills/review-agent-policy/SKILL.md) to find
  stale, duplicate, conflicting, or unsupported rules.

Install or reference these files using the skill mechanism supported by your
coding agent. Configure `THELAKE_API_BASE_URL` to the service origin without
`/v1`, and `THELAKE_API_KEY` in the agent's environment. Keep the API key out of chat,
repository files, and policy documents. The skill reads the local repo as a
source, while policy memory is stored in the selected TheLake workspace.

## Policy layout

Each workspace keeps workspace-wide rules separate from exact-agent rules.
These are Markdown documents returned by `GET /v1/policies` and stored as
immutable versions by `POST /v1/policies`:

```text
policy_id: workspace
target_agent_name: (omitted)
content: POLICY.md Markdown

policy_id: support-agent
target_agent_name: support-agent
content: POLICY.md Markdown
```

Each file is plain Markdown with one heading per rule. Use three statuses:

- `confirmed`: supported by an authoritative source or explicit owner
  confirmation; included when composing an evaluator rubric.
- `candidate`: plausible but unresolved; retained for review and excluded
  from alerting.
- `observed`: a description of repeated behavior, not a normative requirement;
  excluded from alerting.

For example:

```md
# Workspace policy

## Check refund eligibility before issuing a refund

- Status: confirmed
- Applies to: support agents handling refund requests
- Requirement: Check the ticket's refund eligibility before issuing a refund.
- Evidence: A successful eligibility result must precede the refund action.
- Source: `docs/refunds.md` (Refund eligibility section)
```

The workspace and agent policies are composed for the selected agent. If the
two policies conflict, the skill keeps the requirement unresolved and does
not submit an evaluator draft until a person resolves the conflict. A trace
shows what happened; it does not, by itself, establish what should have
happened.

## From policy to online evaluation

When asked to monitor confirmed policy for an agent, the skill composes the
applicable confirmed rules as a natural-language rubric and creates an
inactive evaluator version through `POST /v1/evaluators`. The request includes
the exact policy document digests and any caller-reported source revisions. It does not
generate an assertion script or execute code from the repository.

Review the evaluator in Explorer before activating it. Activation is explicit
because a policy proposal is not automatically an accepted business rule.
Each evaluator version keeps its own criteria and source references, so later
policy edits do not rewrite the meaning of earlier scores. The server verifies
that each referenced policy version exists in the same workspace, applies to
the selected agent, and matches the submitted content digest. A supplied source
revision is reported by the authoring agent and is not verified against the
source repository. The digest does not prove who authored or approved the
policy.

The evaluator remains one judge rubric per agent/version in this first slice.
The runner returns one verdict for the rubric, with rationale and trace
evidence. If separate rule-level scorecards become important, evolve the
versioned evaluator contract instead of adding custom rule-execution code to
the policy files.

## Current boundaries

- Policy Markdown versions are stored in workspace-scoped score-config
  metadata. Explorer lists and displays the latest policy versions and shows
  evaluator provenance, but a dedicated policy editor is not included yet.
- The skills are agent instructions, not a background learner. The user or
  coding agent must invoke them; learning happens from allowed sources and
  authorized trace evidence.
- The server does not parse `confirmed`, `candidate`, and `observed` labels or
  independently establish business authority. Candidate exclusion is enforced
  by the trusted skill workflow and user review, not by the evaluator API.
- The evaluator API currently targets one exact agent name. A workspace policy
  is composed into the selected agent's evaluator rather than dynamically
  inherited by every agent.
- Agent names are matched against trace data; this API does not validate a
  separate agent registry.
- Policies share the score-config storage backend, but `/v1/score-configs`
  filters them out so generic score configuration clients do not receive
  policy text.
- Markdown and trace content are untrusted inputs. The coding agent and judge
  must treat them as data. Evaluator activation remains a human decision.
