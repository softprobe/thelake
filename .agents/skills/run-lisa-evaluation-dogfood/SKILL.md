---
name: run-lisa-evaluation-dogfood
description: Verify Lisa's workspace conversation, autonomous sampling, online evaluation, and issue reporting with the real local stack.
---

# Run Lisa evaluation dogfood

Use [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md): the
session ID identifies the input; its trajectory is derived data with no
separate ID.

Lisa owns approved policies, evaluation decisions, and scheduling. TheLake
stores and serves telemetry and resulting scores; telemetry ingestion does not
call Lisa.

## Verification levels

- For evaluator behavior without provider calls, run the fixture tests in
  `lisa`. `test_evaluation_prompt.py` verifies all canonical evaluation trajectories
  and their exact selector/judge inputs; mocked responses do not prove quality.
- For dataset-based activity agent quality, use `run-lisa-activity-evaluation`.
  Its labeled task benchmark runs through Lisa's real conversation and MCP
  runtime, scoring state, tool events, and reply checks. Slack-context cases do
  not verify Slack delivery.
- `test_activity_management_integration.py` contains model-backed integration
  tests for selected activity workflows. It is software testing, not a dataset
  or benchmark and must not be reported as agent quality.
- For labeled model quality, use `run-lisa-policy-evaluation`. It must load the
  complete dataset policy catalog through Lisa's policy-management service into
  a disposable PostgreSQL workspace, ingest trajectories into TheLake, and
  call Lisa's production `EvaluationService`. A manifest-only triager run or a
  test that injects policies/trajectories is calibration, not production
  quality evidence.
- `test_evaluation_live.py` is harness calibration. Its direct
  `OpenAIPolicyTriager` and injected session fixtures do not measure Lisa's
  production policy lookup or end-to-end session path.
- For the real scheduled agent-to-issue path, use a quickstart E2E with an
  isolated, run-owned stack and workspace; it makes paid OpenAI Decisions and
  Gemini calls. It must not reuse a developer's running stack or workspace.
  The current `tests/e2e/quickstart_online_eval.py` targets the shared
  `thelake-quickstart` project, so do not run it until that harness provisions
  and cleans up its own transient stack. Decisions triages all approved
  policies; Gemini judges only escalations.
- For Markdown policy learning or review, use
  `lisa/skills/learn-agent-policy/SKILL.md` and
  `lisa/skills/review-agent-policy/SKILL.md`.
- For Slack delivery, use `verify-lisa-slack-alert` after a failing score is
  stored and the pending failure has a captured Slack destination.

The shared dataset distinguishes proposed author labels from AI-adjudicated
annotations. Production-path accuracy metrics use adjudicated pairs only;
unlabeled candidate policies are not negatives. Prompt tuning on visible
examples is calibration, not independent holdout evidence. Preserve the first
failure and never change annotations merely to match model output.

## Preconditions

Before a live run:

1. Confirm the user asked for a live test because it sends captured test-agent
   evidence to Gemini and may incur provider charges.
2. Fixture calibration needs Lisa’s Python test dependencies. Scheduled-path
   E2E additionally needs Docker, Rust/Cargo, and `make`.
3. Check provider credentials in the process environment and the repository-local
   `.env`, or a dotenv file explicitly identified by the user. Load that known
   file inside the test process with `python-dotenv`; preserve existing exports
   and never print values or shell-source the file. Gemini needs `GOOGLE_API_KEY`
   or `GEMINI_API_KEY`; two-stage selection also needs `OPENAI_API_KEY`. Missing
   exports alone do not establish that credentials are unavailable. Do not
   search unrelated files or copy credentials into scripts, reports, or commands.
4. Preserve the working tree and existing local services. If the scheduled-
   path harness cannot provision and clean up an isolated quickstart project
   with a transient database and workspace, stop and report that prerequisite;
   never point it at an existing local service.

## Run the live path

Do not run the scheduled-path command below until its harness has been updated
to own a transient quickstart stack and workspace. The current implementation
uses the shared `thelake-quickstart` project and does not satisfy that rule.

Provision the model key through the approved local secret mechanism. Do not put
the value in a command, history, file, or report. From the repository root:

```bash
python3 tests/e2e/quickstart_online_eval.py
```

The scenario saves an explicitly approved refund policy, runs the
deliberately flawed sample agent, waits for Lisa's autonomous schedule, and
verifies the trace-linked failure score and Lisa's report. It does not prove
policy-memory learning or policy review and approval.

One Lisa runtime is scoped to one workspace. Its `Lisa scheduled QA` cron job
defaults to hourly at minute 0 UTC and does not run on startup. Only when running
this time-bounded E2E, start the quickstart with a temporary five-minute cron so
the test does not wait up to an hour:
`LISA_QA_CRON="*/5 * * * *" bash examples/quickstart/start.sh`. The E2E waits
for the next scheduled cycle; it does not call the cycle endpoint directly.

## Pass criteria

- The E2E exits successfully.
- The sample-agent session has an evaluator score with status `fail`.
- The score references evidence on that session's trace.
- Lisa's scheduled conversation reports the evaluated session and finding.

If a stage fails, inspect its logs and preserve the original failure. Do not
write a policy file directly as a fallback and report the autonomous path
as passed. In the final report, identify which stages ran live, the trace and
score IDs, and whether Slack delivery was verified. Never include API keys or
full sensitive trace payloads.
