# Repeatable policy and online-evaluation dogfood

Use this small travel scenario to check three different claims independently:

1. A coding agent can learn a supported rule from an authoritative source.
2. The Gemini/DeepEval runner distinguishes compliant behavior from a real
   violation and refuses to pass incomplete evidence.
3. Lisa selects a captured session, runs the check, stores the score in
   TheLake, and reports a failure with its evidence.

This separation matters: a good judge result does not prove the learning skill
found the right rule, and a successful skill run does not prove online trace
evaluation works.

Codex and Cursor project skills for these developer workflows live in
`.agents/skills/`: `run-lisa-evaluation-dogfood` exercises the local live path,
and `verify-lisa-slack-alert` checks the optional Slack notification path.
These developer skills are separate from Lisa's policy authoring skills in
`skills/`.

## 1. Check policy learning

Invoke `learn-agent-policy` with the files in
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

## 2. Calibrate the live judge

Run the paired calibration cases through the same local evaluator Lisa uses:

```bash
uv run --project lisa-agent python lisa-agent/scripts/run_policy_dogfood.py \
  --output /tmp/thelake-policy-dogfood.json
```

Lisa evaluates three fixed cases from
[`connected_itinerary.json`](../../lisa-agent/tests/fixtures/connected_itinerary.json):

- A policy-compliant disclosure before confirmation: expected `pass`.
- A cancellation followed by a late warning: expected `fail`.
- An incomplete trace: expected `insufficient_evidence`.

The command exits nonzero on a verdict mismatch. Its optional report contains
case IDs, scores, rationale, timings, and evidence span IDs; it does not include
the full trace payload or credentials. Gemini results are live model judgments,
so rerun the suite when changing rubric wording, judge configuration, or trace
evidence. Treat repeated false positives or unstable verdicts as a rubric/trace
quality issue, not as a passing evaluation.

The fixture contract can be checked without a Gemini call:

```bash
uv run --project lisa-agent --extra test pytest lisa-agent/tests/test_dogfood_fixture.py
```

## 3. Verify Lisa's scheduled QA cycle

Start the quickstart with the five-minute cron shown in [the quickstart](../quickstart.md), then run:

```bash
python3 tests/e2e/quickstart_online_eval.py
```

The driver creates and activates a check, runs the deliberately flawed Gemini
agent, waits for Lisa's configured schedule to pick up its trace, and verifies
the persisted failure score plus Lisa's report. It makes provider calls and
may incur Gemini charges. The test agent only simulates a local refund tool;
it does not connect to a payment system.

## Interpreting the result

Report the stages separately. Do not call the system successful if only the
judge fixture passes. A useful dogfood run records:

- Whether the learning skill proposed the expected policy and cited its source.
- Expected and actual verdict for every calibration case.
- Whether the live application E2E persisted a score for the ingested trace.
- Whether the issue view showed the failure and linked evidence to the source
  trace.

The dogfood override fires `Lisa scheduled QA` every five minutes through a
cron trigger. Normal deployments default to hourly at minute 0 UTC. Each fire
starts one model turn and evaluates at most one unchecked session; it is not a
guarantee of complete production coverage. The cron registration is process
local, and a Lisa restart can miss a scheduled fire. A durable work queue and
grouped issue records remain part of the product design.
