---
name: run-lisa-evaluation-dogfood
description: Verify Lisa's policy evaluation of a pushed session with an isolated local runtime.
---

# Run Lisa evaluation dogfood

Use [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md): the
session ID identifies the input; its trajectory is derived data with no
separate ID.

Lisa is an independently deployable Python service. It serves its own frontend
and API, stores policy/evaluation state in its own PostgreSQL database, and
stores OTLP evidence in its durable agent workspace. TheLake sampling is an
optional Lisa-owned integration; the isolated evaluation procedure below uses
local evidence and does not require TheLake.

## Verification levels

- For evaluator behavior without provider calls, run Lisa fixture tests.
  `test_evaluation_prompt.py` verifies canonical evaluation trajectories and
  selector/judge inputs; mocked responses do not prove model quality.
- For dataset-based activity agent quality, use
  `run-lisa-activity-evaluation`. Slack-context cases do not verify delivery.
- For labeled model quality, use `run-lisa-policy-evaluation`. The labeled
  benchmark must load the complete policy catalog through Lisa's policy
  service and evaluate locally stored sessions through `EvaluationService`.
- For Slack delivery, use `verify-lisa-slack-alert` after a failing result is
  stored and the pending failure has a captured Slack destination.

The shared dataset distinguishes proposed labels from adjudicated
annotations. Use adjudicated pairs only for accuracy. Unlabeled candidate
policies are not negatives. Preserve initial failures and do not change labels
to match model output.

## Push contract

A producer on a private network submits a complete OTLP JSON session to Lisa's
`POST /internal/v1/session-traces` endpoint. The endpoint has no bearer token;
deployment must prevent public ingress. Session identity follows `sp.session.id`, then
`gen_ai.conversation.id`, then trace ID. Agent identity follows
`sp.agent.name`, then `service.name`. Lisa validates the bundle, atomically
writes `session_traces/<session_id>/otlp.json` and its manifest, and inserts a
PGMQ message with a durable receipt in Lisa Postgres. Repeating identical
content is safe; the same session ID with different content is rejected.
A successful HTTP receipt means the bundle and queue message are durable; check
the evaluation activity for the eventual outcome.

## Preconditions for a live run

1. Run only when the user requests live provider evaluation. It sends test
   evidence to Gemini and may incur provider charges.
2. Use an isolated stack, database, and filesystem owned by the run. Never
   attach an automated test to the persistent developer environment.
3. Check provider credentials in process environment and repository `.env`
   without printing values or sourcing the file. Gemini needs
   `GOOGLE_API_KEY` or `GEMINI_API_KEY`; two-stage triage also needs
   `OPENAI_API_KEY`.
4. Use Lisa's own local session-trace directory; do not provision TheLake or
   call its trace API for the evaluation.

For a live benchmark, follow the isolated run procedure in
[`run-lisa-policy-evaluation`](../run-lisa-policy-evaluation/SKILL.md). The
sample agent only simulates a local refund tool and never connects to a payment
system.

## Pass criteria

- The session is accepted and can be read back from Lisa's evidence store.
- A queued evaluation activity reaches a terminal outcome.
- The result references the expected session and approved policy IDs.
- Any Slack delivery is verified separately and is not inferred from a stored
  evaluation result.

Never claim live agent quality from fixture tests or a direct triager/judge
call that bypasses Lisa's production service. Never include API keys, bot
tokens, or full sensitive trace payloads in reports.
