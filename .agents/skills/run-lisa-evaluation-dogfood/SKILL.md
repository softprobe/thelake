---
name: run-lisa-evaluation-dogfood
description: Verify Lisa's workspace conversation, autonomous sampling, online evaluation, and issue reporting with the real local stack.
---

# Run Lisa evaluation dogfood

Lisa is the proactive agent. TheLake stores and serves telemetry, evaluator
definitions, and scores; telemetry ingestion does not call Lisa.

## Verification levels

- For evaluator behavior without provider calls, run the fixture tests in
  `lisa-agent`.
- For the real scheduled agent-to-issue path, run the quickstart E2E. It uses
  the existing quickstart stack and makes paid Gemini calls.
- For Markdown policy learning or review, use
  `skills/learn-agent-policy/SKILL.md` and
  `skills/review-agent-policy/SKILL.md`.
- For Slack delivery, use `verify-lisa-slack-alert` after a failing score is
  stored and the evaluator has a Slack thread binding.

## Preconditions

Before a live run:

1. Confirm the user asked for a live test because it sends captured test-agent
   evidence to Gemini and may incur provider charges.
2. Confirm Docker, Rust/Cargo, Python, and `make` are available.
3. Check whether `GOOGLE_API_KEY` or `GEMINI_API_KEY` is present without
   printing, copying, or reading its value. If absent, report that the live
   test cannot run; do not search files for credentials.
4. Preserve the working tree and existing local services. Start the local
   quickstart first if it is not already running.

## Run the live path

Provision the model key through the approved local secret mechanism. Do not put
the value in a command, history, file, or report. From the repository root:

```bash
python3 tests/e2e/quickstart_online_eval.py
```

The scenario creates and activates a refund behavior check, runs the
deliberately flawed sample agent, waits for Lisa's autonomous schedule, and
verifies the trace-linked failure score and Lisa's report. It does not prove
policy-memory learning or policy review and approval.

One Lisa runtime is scoped to one workspace. Its `Lisa scheduled QA` cron job
defaults to hourly at minute 0 UTC and does not run on startup. For a faster
dogfood run, start the quickstart with a short explicit cron schedule, such as
`LISA_QA_CRON="*/5 * * * *" bash examples/quickstart/start.sh`. The E2E waits
for the next scheduled cycle; it does not call the cycle endpoint directly.

## Pass criteria

- The E2E exits successfully.
- The sample-agent session has an evaluator score with status `fail`.
- The score references evidence on that session's trace.
- Lisa's scheduled conversation reports the evaluated session and finding.

If a stage fails, inspect its logs and preserve the original failure. Do not
call the evaluator API directly as a fallback and report the autonomous path
as passed. In the final report, identify which stages ran live, the trace and
score IDs, and whether Slack delivery was verified. Never include API keys or
full sensitive trace payloads.
