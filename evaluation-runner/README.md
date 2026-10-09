# thelake evaluation runner

This service executes versioned, natural-language behavior checks against a
bounded trace evidence payload. It uses DeepEval's `ConversationalGEval` for
semantic conversation checks and deterministic predicates for required tool
ordering. The thelake API owns evaluator definitions, trace reads, and durable
trace-linked score writes; this service only evaluates the request it receives.

The service is intended for private service-to-service traffic. Configure
`EVALUATION_RUNNER_TOKEN` and `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) before
starting it. The runner rejects evaluation requests without bearer
authentication. Do not expose its port directly to untrusted networks.

Run locally from this directory:

```bash
python -m venv .venv
. .venv/bin/activate
pip install .
export EVALUATION_RUNNER_TOKEN=local-runner-token
export GOOGLE_API_KEY=your-key
uvicorn thelake_evaluation_runner.api:app --app-dir src --host 127.0.0.1 --port 8081
```

`POST /v1/evaluate` accepts one evaluator version and one trace's bounded,
ordered user/assistant/tool evidence. Results distinguish pass, fail,
uncertain, and insufficient evidence; provider failures return an HTTP error. Evidence references
retain source span IDs. The request is limited to 1 MB of serialized evidence.
It also accepts at most 1,000 events and 120,000 characters of combined
conversation and tool payload; the HTTP request body limit is 1.1 MB.
Common credential fields, bearer tokens, and provider-style API keys are
redacted before the provider call. This is a narrow secret filter, not general
PII scrubbing. Tool-order rules compare event sequence and correlate a tool
result by call ID (or span ID when no call ID is present). Missing tool capture,
ambiguous ordering, or an uncorrelated prerequisite result yields
`insufficient_evidence`; an observed consequential action without its
prerequisite is a failure.

## Creating a behavior evaluator

Create a versioned evaluator through the authenticated thelake API. The rule
describes the expected behavior; traces are evaluated automatically after
ingestion when the runner is configured.

For local Compose, set the runner URL to
`http://evaluation-runner:8081/v1/evaluate`, set the same random
`THELAKE_EVALUATION_RUNNER_TOKEN` for both services, provide `GOOGLE_API_KEY`,
then start with `docker compose --profile evaluation up --build`.

```json
{
  "evaluator_id": "refund-eligibility",
  "version": 1,
  "target_agent_name": "support-agent",
  "name": "Check eligibility before refund",
  "criteria": "Verify the agent checks refund eligibility and explains the result before issuing a refund.",
  "required_tool_order": [
    {
      "before": "check_eligibility",
      "action": "issue_refund",
      "require_result_before_action": true
    }
  ]
}
```

This creates a draft. Activate it with
`POST /v1/evaluators/refund-eligibility/versions/1/activate` to evaluate new
matching traces, or use the `/deactivate` endpoint to stop future evaluations.

The deterministic order rule catches a missing or late prerequisite call when
tool-call evidence is present. A prerequisite call without a correlated result
returns insufficient evidence. The G-Eval judge evaluates the natural-language
behavior across the conversation. Scores link back to the trace and include the
judge rationale, source span IDs, and evidence limitations.

## Repeatable dogfood cases

[`dogfood/connected_itinerary.json`](dogfood/connected_itinerary.json) contains
paired compliant and violating traces plus an incomplete-evidence control. Run
them against the live authenticated runner with:

```bash
export THELAKE_EVALUATION_RUNNER_URL=http://127.0.0.1:8081/v1/evaluate
export THELAKE_EVALUATION_RUNNER_TOKEN=local-runner-token
python scripts/run_policy_dogfood.py --output /tmp/thelake-policy-dogfood.json
```

The runner must have Gemini credentials configured. The script exits nonzero
when an expected verdict differs; it does not substitute a mocked judge. See
the [dogfood guide](../docs/how-to/policy-dogfood.md) for the complementary
policy-authoring and full online-pipeline checks.
