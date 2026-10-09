---
name: run-lisa-evaluation-dogfood
description: Run and report TheLake's local online-evaluation dogfood, including live Gemini rubric calibration, Explorer chat check creation, agent trace capture, and issue verification. Use when a developer wants to verify the current Lisa evaluation path.
---

# Run Lisa evaluation dogfood

Use this skill to verify the implemented TheLake evaluation path and report
unsupported parts of the intended Lisa workflow as gaps. Do not describe the
current system as a persistent Lisa agent: Lisa's durable workspace home and
background learner are not implemented. Explorer chat currently creates a
behavior evaluator, not a `POLICY.md` memory.

## Choose the verification level

- For a deterministic local test without provider calls, run the dogfood
  fixture tests in `evaluation-runner`.
- For the real agent-to-issue path, run the online E2E. It starts an isolated
  local stack and makes paid Gemini calls.
- For Markdown policy learning or review, use the separate
  `skills/learn-agent-policy/SKILL.md` and
  `skills/review-agent-policy/SKILL.md`. Those are authoring workflows; they
  require a developer-provided private durable directory and are not an
  automated step in the online E2E.
- For Slack delivery, use the `verify-lisa-slack-alert` skill. The online E2E
  does not configure Slack or assert a Slack message.

## Preconditions

Before starting a live run:

1. Confirm the user explicitly asked to run the live test because it sends
   captured test-agent evidence to Gemini and may incur provider charges.
2. Confirm Docker, Rust/Cargo, Node/npm, Python, and `make` are available.
3. Check whether `GOOGLE_API_KEY` or `GEMINI_API_KEY` is present without
   printing, copying, or reading its value. If absent, stop and report that the
   live test cannot run; do not search files for credentials.
4. Check the working tree and preserve all existing developer changes. The
   test script owns its temporary database, runner, server, and browser
   artifacts. It removes its local temporary data by default; if
   `THELAKE_E2E_KEEP_ARTIFACTS=1` is set, it preserves logs and temporary data
   for inspection.

## Run the live path

Provision `GOOGLE_API_KEY` or `GEMINI_API_KEY` through the approved local
secret mechanism before running. Do not put the value in a shell command,
history, file, or report. From the repository root, keep the normal 60-second
sampling interval unless the user requested a different sampling test:

```bash
make test-explorer-online-e2e
```

The command runs the three paired travel-rubric cases, then the real-stack
Explorer browser E2E. The browser scenario creates and activates a refund
behavior check through Explorer chat, runs the deliberately flawed sample
agent, and verifies the trace-linked failure score and issue view. It proves
the current automatic evaluation path; it does not prove policy-memory
learning, policy review/approval, or Slack delivery.

The default sampler admits the first eligible trace per workspace and
authenticated agent every 60 seconds per process. Setting
`THELAKE_EVALUATION_SAMPLE_INTERVAL_SECONDS=0` disables interval sampling; it
can make a fixture more deterministic, but then the run does not verify
sampling. The existing online E2E does not assert that a second same-agent
trace is skipped, so report sampling behavior as unverified unless you run two
sequential traces with the same authenticated agent and confirm only the first
gets an evaluator score.

## Decide whether the run passed

Treat each stage separately. A passing fixture calibration is not proof of
automatic trace evaluation. For a live-path pass, require all of the following:

- The online command exits successfully.
- The ingested sample-agent trace has an evaluator score with status `fail`.
- The score references evidence on that trace.
- Explorer displays the behavior issue and can open the linked trace.

If a stage fails, inspect the failing stage's logs and preserve the original
failure. Do not call the evaluation API directly as a fallback and report the
automatic path as passed. In the final report, identify which stages ran live,
which were skipped, the trace and score IDs, and any missing Lisa or Slack
capabilities. Never include API keys or full sensitive trace payloads.
