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

## 2. Calibrate the live judge

Run the paired calibration cases through the same local evaluator Lisa uses:

```bash
uv run --project lisa python lisa/scripts/run_policy_dogfood.py \
  --output /tmp/thelake-policy-dogfood.json
```

Lisa evaluates three fixed cases from
[`connected_itinerary.json`](../../lisa/tests/fixtures/connected_itinerary.json):

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
uv run --project lisa --extra test pytest lisa/tests/test_dogfood_fixture.py
```

### STATE-Bench gold formatting and model quality

The five cases in `lisa/tests/fixtures/state_bench_gold.jsonl` have policy-selection
labels, verdict labels, and illustrative supporting spans. Verify exact event
content, tool linkage, and provider-boundary prompts without provider calls:

```bash
uv run --project lisa --extra test pytest \
  lisa/tests/test_state_bench_prompt.py \
  lisa/tests/test_prompt_citations.py \
  lisa/tests/test_tool_result_format.py \
  lisa/tests/test_trajectory_prompt.py -v
```

For live quality, load the known local `.env` into the test process without
printing secrets. Existing exported credentials take precedence. This invokes
paid Gemini calls, plus OpenAI Decisions calls for the two-stage checks:

```bash
uv run --project lisa --extra test python - <<'PY'
import os
from dotenv import load_dotenv
import pytest

load_dotenv(".env", override=False)
os.environ["LISA_RUN_STATE_BENCH_LIVE"] = "1"
raise SystemExit(pytest.main([
    "lisa/tests/test_state_bench_policy_selection_live.py", "-v", "-rs",
]))
PY
```

Each case reports separately for judge-only quality and for selection through
Lisa's MCP `evaluate_session` tool. The selection test supplies fixture-backed
session and approved-policy data, then exercises Lisa's production controller
and evaluation service. It checks that the service records one Gemini judge
request and score per selected policy before asserting the gold policy selection;
an over-selection therefore remains visible without preventing its extra judge
call from being exercised. The fixture trajectory is injected at Lisa's session
trajectory boundary, so this live check does not exercise raw TheLake span
conversion or upstream completeness gating. Tests also check gold verdict
direction, nonempty rationale, and citations that belong to the trajectory;
valid alternate citations need not match the illustrative gold span exactly.
Missing credentials produce explicit skips, which are not successful quality
checks. These five trajectories fit without clipping, so their results validate
compact-input quality on this fixture set; they do not establish quality after
clipping or no regression against the old JSON prompt.

The judge defaults to Gemini's `low` thinking level to reduce reasoning-token
cost. Set `LISA_GEMINI_THINKING_LEVEL=medium` or `high` when calibration shows
that the lower-cost setting misses expected verdicts. The live gold suite is
the quality check for this tradeoff. Lisa does not set a hard output-token cap:
Gemini counts reasoning tokens against that cap, and exhausting it can truncate
the required structured response.
The current `gemini-3.8-flash` judge uses implicit caching only, which requires
at least 4,096 input tokens; compact gold prompts are below that threshold, so
they should not be expected to report cached input tokens.

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
