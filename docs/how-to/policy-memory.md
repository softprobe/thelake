# Policy memory for agent behavior checks

Lisa keeps business context as short, readable Markdown in her durable
workspace home. Use one workspace `POLICY.md` for shared rules and an optional
`agents/<sha256-of-exact-agent-name>/POLICY.md` for rules that apply to one
agent. Record the exact agent name inside the file. Never use telemetry names
directly as filesystem paths or follow symlinks outside Lisa's home. This is a
file convention, not a new policy API or a second git repository.

The reusable skills are:

- [`learn-agent-policy`](../../skills/learn-agent-policy/SKILL.md) to gather
  evidence, update Markdown notes, and draft an evaluator.
- [`review-agent-policy`](../../skills/review-agent-policy/SKILL.md) to find
  stale, duplicate, conflicting, or unsupported rules.

## Write reviewable rules

Keep each rule short and include what must happen, when it applies, what trace
evidence can establish it, and where it came from:

```md
# Workspace policy

## Check refund eligibility before issuing a refund

- Status: confirmed
- Applies to: support agents handling refund requests
- Requirement: Check ticket eligibility before issuing a refund.
- Evidence: A successful eligibility result must precede the refund action.
- Source: `docs/refunds.md` (Refund eligibility section)
```

Use `candidate` for an unresolved interpretation and `observed` for a pattern
that describes behavior but does not establish what the agent should do. A
trace alone is not proof of business intent. A person must confirm a rule
before it can become an active check; a model's confidence or a source link
alone is not approval.

When rules conflict, preserve the sources and leave the affected requirement
as a candidate until a person resolves it. Do not copy credentials or private
data into policy files.

## Turn policy into an online check

When asked to monitor policy, the skill composes the applicable confirmed
rules and their citations into the evaluator's natural-language `criteria`.
The existing evaluator API versions that criteria, and the existing online
runner evaluates new sessions. This keeps policy authoring out of customer
code and avoids maintaining a separate evaluator script repository.

Review the exact rubric and activate it explicitly in Explorer. Because the
source citation is included in the versioned criteria, later edits to
`POLICY.md` do not silently change what an earlier evaluator means. The API
does not verify the author-provided source citation or parse policy status; the
skill and reviewer are authoring aids, and human review/activation is the trust
boundary.

## Current boundary

The public repository includes policy examples and reusable learning/review
skills, along with the evaluator API and online runner. It does not yet provide
Lisa's persistent workspace home or an automatically learning background
agent. Until that home exists, a host agent must supply a durable, private
directory for these Markdown files; do not store them in the customer's app
repository. The separate [Lisa product design](../architecture/lisa-agent-qa.md)
describes the target workspace agent and its responsibilities.
