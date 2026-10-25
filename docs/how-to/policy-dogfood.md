# Repeatable policy and online-evaluation dogfood

Use [Lisa evaluation terms](../architecture/lisa-terms.md) when referring to
sessions, derived trajectories, policies, and evaluation pairs.

Use this small travel scenario to check three different claims independently:

1. A coding agent can learn a supported rule from an authoritative source.
2. Decisions triage routes possible violations and unresolved evidence to the
   categorical Gemini judge, which distinguishes compliance from a real
   violation and refuses to pass incomplete trajectories.
3. Lisa selects a pushed session, runs the check, stores the evaluation in
   Lisa Postgres, and reports a failure with its evidence.

This separation matters: a good judge result does not prove the learning skill
found the right rule, and a successful skill run does not prove online trace
evaluation works.

Codex and Cursor project skills for these developer workflows live in
`.agents/skills/`: `run-lisa-evaluation-dogfood` exercises the local live path,
and `verify-lisa-slack-alert` checks the optional Slack notification path.
These developer skills are separate from Lisa's runtime skills in
[`lisa/skills/`](../../lisa/skills/).

## 1. Check policy learning

Invoke `learn-agent-policy` from
[`lisa/skills/learn-agent-policy/SKILL.md`](../../lisa/skills/learn-agent-policy/SKILL.md)
with the files in
[`examples/policy-dogfood/connected-itinerary/source/`](../../examples/policy-dogfood/connected-itinerary/source/)
as the authoritative business source. Ask it to learn the behavior for a travel
agent and explain which statements are confirmed versus unresolved. Compare the
proposed Markdown with the canonical
[`POLICY.md`](../../examples/policy-dogfood/connected-itinerary/POLICY.md).

Review for these required points:

- Identify the same-day onward segment from the full itinerary.
- Explain that cancelling the first leg could strand the traveler.
- Give the warning before asking for confirmation and before cancellation.
- Cite the business source. Do not use the failing trace itself as proof of
  policy.

Keep this as a human-reviewed stage while the skill runs inside each user's
coding agent. TheLake does not currently execute the policy-learning skill or
score its output automatically.

## 2. Calibrate the evaluator harness

All examples use the [shared evaluation dataset](../../lisa/datasets/policy_evaluation/README.md).
Itinerary controls include disclosure before confirmation, cancellation with a
late warning, and incomplete capture. Check each pair's review status in the
shared dataset and use only `adjudicated` rows as quality labels. The release is
AI-adjudicated rather than human-reviewed.

Check the shared data and provider wiring without provider calls:

```bash
uv run --project lisa --extra test pytest -q \
  lisa/tests/test_evaluation_dataset.py \
  lisa/tests/test_evaluation_prompt.py
```

The dataset also contains ten STATE-Bench trajectories. Only each authored target
pair is annotated; other catalog pairs remain unreviewed.

For labeled production-path quality, use the
[production-path evaluation skill](../../.agents/skills/run-lisa-policy-evaluation/SKILL.md),
which loads the full policy catalog through Lisa's policy-management service and
calls `EvaluationService` against Lisa's local evidence. Metrics and limits:
[the evaluation guide](policy-triage-evaluation.md).

## 3. Push-triggered online evaluation

This isolated dogfood procedure evaluates locally stored Lisa evidence and does
not require TheLake. Lisa can optionally sample TheLake through its own
authenticated scheduler and packaged skill when that integration is configured;
sampled bundles use the same durable intake as direct pushes.
The telemetry producer submits a complete OTLP session to Lisa's unauthenticated
`POST /internal/v1/session-traces` endpoint, which must remain on a private
network. Lisa atomically writes the session bundle under its configured
evidence directory, then commits the receipt and evaluation message together
in its own PGMQ-enabled PostgreSQL database. A successful API response confirms
durable receipt and queue insertion, not evaluation completion or an atomic
transaction across the filesystem and database.

For a live evaluation, use the isolated stack and dataset flow in the
[policy evaluation skill](../../.agents/skills/run-lisa-policy-evaluation/SKILL.md).
The run makes provider calls and may incur charges. The sample agent only
simulates a local refund tool and never connects to a payment system. The
interactive developer environment is documented in
[Local Lisa development](local-development.md).

## Interpreting the result

Report the stages separately. Do not call the system successful if only the
judge fixture passes. A useful dogfood run records:

- Whether the learning skill proposed the expected policy and cited its source.
- Expected and actual verdict for every calibration case.
- Whether the live application E2E persisted its evaluation result in Lisa
  Postgres for the ingested trace.
- Whether Lisa's activity shows the failure and links to the locally stored
  session evidence.

For multi-policy synthetic cases, recall metrics, and production dataset design,
see [Policy triage evaluation](policy-triage-evaluation.md).
