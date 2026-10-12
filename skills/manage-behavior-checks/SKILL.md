---
name: manage-behavior-checks
description: Turn an owner-approved named policy into a Lisa evaluation and identify coverage gaps during proactive QA.
---

# Manage behavior checks

Use this skill when an owner asks Lisa to monitor an approved rule or when
`own-agent-qa` finds an approved policy without an active evaluation.

1. Identify the exact agent from the policy's scope or a completed root trace.
   Ask one focused question if the target or requested behavior is ambiguous.
   During scheduled QA, report the ambiguity and continue with other work.
2. Read relevant workspace or agent `POLICY.md` notes and list active
   evaluations. Reuse an existing evaluation with the same name, agent, and
   rubric.
3. Write a concise natural-language rubric that names the required behavior,
   trace evidence that can establish it, and when the verdict must be
   inconclusive. Do not invent business rules from a trace.
4. Present any new or changed rule to the owner and wait for explicit approval
   before saving it. Create an inactive named evaluation only for approved
   policy. The owner activates it in Explorer or with the explicit command
   `Activate evaluation: <exact policy name> for agent: <exact agent name>`.
   Scheduled QA can report a
   coverage gap but cannot create or activate an evaluation.
5. When the conversation came from Slack, use the trusted channel and thread
   context attached to Lisa's tools so failures return to that same thread.

Never claim an evaluation is active until the activation tool confirms it.
Never claim a session passed unless Lisa evaluated it and stored a passing
verdict.
