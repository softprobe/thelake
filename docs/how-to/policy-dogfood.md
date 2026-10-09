# Repeatable policy and online-evaluation dogfood

Use this small travel scenario to check three different claims independently:

1. A coding agent can learn a supported rule from an authoritative source.
2. The Gemini/DeepEval runner distinguishes compliant behavior from a real
   violation and refuses to pass incomplete evidence.
3. TheLake's online path persists trace-linked evaluator scores and displays
   failures.

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

Start the evaluation runner with Gemini credentials and its service token, then
point this command at its authenticated endpoint:

```bash
export THELAKE_EVALUATION_RUNNER_URL=http://127.0.0.1:8081/v1/evaluate
export THELAKE_EVALUATION_RUNNER_TOKEN=local-runner-token
python3 evaluation-runner/scripts/run_policy_dogfood.py \
  --output /tmp/thelake-policy-dogfood.json
```

The runner evaluates three fixed cases from
[`connected_itinerary.json`](../../evaluation-runner/dogfood/connected_itinerary.json):

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
cd evaluation-runner
.venv/bin/pytest tests/test_dogfood_fixture.py
```

## 3. Verify TheLake's online path

Run the existing real-stack browser test:

```bash
make test-explorer-online-e2e
```

It starts an isolated Postgres catalog, TheLake, and the live evaluation
runner; creates and activates a behavior check in Explorer; runs a deliberately
flawed Gemini agent; then waits for the real trace-linked failure score and
checks the Explorer issue view. It makes provider calls and may incur Gemini
charges. In online mode the script also runs the three paired travel rubric
cases from step 2 against that same live runner before starting the browser
tests. The test agent only simulates a local refund tool; it does not connect
to a payment system.

The online browser E2E currently uses its refund-eligibility scenario. It proves
trace capture → automatic runner call → persisted score → visible issue. The
connected-itinerary fixture above calibrates the policy rubric and judge
directionality. A follow-up should run the STATE-Bench task agent through this
same automatic trace-triggered path, rather than its older post-run evaluator
helper, and assert the evaluator score and evidence against the benchmark's
expected behavior.

## Interpreting the result

Report the stages separately. Do not call the system successful if only the
judge fixture passes. A useful dogfood run records:

- Whether the learning skill proposed the expected policy and cited its source.
- Expected and actual verdict for every calibration case.
- Whether the live application E2E persisted a score for the ingested trace.
- Whether the issue view showed the failure and linked evidence to the source
  trace.

The online scheduler is best effort, so a timeout is a failed run with a
diagnostic—not a reason to fall back to a manual evaluator call and report
success.

Automatic evaluation currently admits the first eligible trace per workspace
and authenticated agent every 60 seconds, per service process. This is a
rate cap, not a representative random sample; each replica has an independent
window. Set
`THELAKE_EVALUATION_SAMPLE_INTERVAL_SECONDS=0` in the dogfood environment to
disable time-window sampling so it does not affect the fixture. The process
worker cap still applies. The default is useful for a light online smoke test,
not for proving full-traffic coverage. A runner failure consumes the current
interval.
