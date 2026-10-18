# Repeatable policy and online-evaluation dogfood

Use this small travel scenario to check three different claims independently:

1. A coding agent can learn a supported rule from an authoritative source.
2. The categorical Gemini judge distinguishes compliant behavior from a real
   violation and refuses to pass incomplete trajectories.
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

## 2. Calibrate the evaluator

All examples use the [shared evaluation dataset](../../lisa/datasets/policy_evaluation/README.md).
Itinerary controls include disclosure before confirmation, cancellation with a
late warning, and incomplete capture. Their labels are proposed author judgments,
not independently adjudicated gold. Run those explicit calibration checks with
paid Gemini calls:

```bash
LISA_RUN_EVALUATION_LIVE=1 uv run --project lisa --extra test pytest -q \
  lisa/tests/test_evaluation_live.py \
  -k "pair_judge_calibration and connected_itinerary"
```

Check the shared data and provider wiring without provider calls:

```bash
uv run --project lisa --extra test pytest -q \
  lisa/tests/test_evaluation_dataset.py lisa/tests/test_evaluation_prompt.py
```

The dataset also contains ten STATE-Bench trajectories. Only each authored target
pair is annotated; other catalog pairs remain unreviewed. All provider formatting
and calibration tests resolve the same canonical trajectory and policy records.

For live judge calibration and the MCP selection-to-score path, load the known
local `.env` inside the test process, preserving existing exports and never
printing secrets. This invokes paid Gemini and OpenAI calls:

```bash
uv run --project lisa --extra test python - <<'PYCODE'
import os
from dotenv import load_dotenv
import pytest

load_dotenv(".env", override=False)
os.environ["LISA_RUN_EVALUATION_LIVE"] = "1"
raise SystemExit(pytest.main(["lisa/tests/test_evaluation_live.py", "-v", "-rs"]))
PYCODE
```

Judge calibration compares explicit annotations, including proposed author
expectations. The MCP path checks every adjudicated violation survives triage,
one judge request and score per escalation, and valid citations. It injects the
trajectory at the session boundary; raw telemetry conversion is tested separately.
Skipped tests do not establish quality. These short examples do not establish
production recall. See [the evaluation guide](policy-triage-evaluation.md) for
metrics scored only against adjudicated annotations and dataset expansion.

## 3. Verify Lisa's scheduled QA cycle

Start the quickstart with the five-minute cron shown in [the quickstart](../quickstart.md), then run:

```bash
python3 tests/e2e/quickstart_online_eval.py
```

The driver saves an explicitly owner-approved policy, runs the deliberately flawed Gemini
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

For multi-policy synthetic cases, recall metrics, and production dataset design,
see [Policy triage evaluation](policy-triage-evaluation.md).
