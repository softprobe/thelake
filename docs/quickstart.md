# 5-minute quickstart

Walk through TheLake's online evaluation yourself: start the local stack, create
a natural-language behavior check, run a deliberately flawed support agent,
then inspect its trace and evaluation in Explorer and ask Lisa to investigate.
Initial builds and image downloads can take longer than five minutes.

The sample agent issues a refund without checking eligibility. Its refund tool
returns a local demo result and is not connected to a payment system.

## Before you start

Install Docker with Compose, Rust, Node.js 24 LTS with npm, Python 3, `make`, and
`curl`. Put API keys with available credits in the gitignored `.env` file:
`GEMINI_API_KEY=...` for the sample agent and Lisa's judge, and
`OPENAI_API_KEY=...` for Lisa's Decisions policy triage. The start script prompts
for Gemini credentials if they are missing; set the OpenAI key in `.env` before
running the scheduled evaluation. Both providers receive live requests.

## 1. Start TheLake

From the repository root, run:

```bash
LISA_QA_CRON="*/5 * * * *" bash examples/quickstart/start.sh
```

The script generates a local Lisa service token, starts Postgres and Lisa, then
runs TheLake in the foreground. This five-minute cron is for the walkthrough;
deployments default to hourly. Lisa first sends every approved candidate policy
to OpenAI Decisions for triage. It sends policies marked
`suspected_violation` or `insufficient_evidence` to the categorical Gemini
judge; `no_violation_found` does not produce a pass score. It prints the
Explorer URL and the next commands. Keep this terminal open and open the printed
URL.

## 2. Approve the policy

In Explorer, open Lisa's chat and send:

> Propose a policy named "Refund eligibility before refund" for `quickstart-refund-agent`: check refund eligibility and wait for the result before issuing a refund. Show the readable policy section with `approved: yes`; do not save it yet.

Review Lisa's proposal, then send:

> Confirm policy rule: Refund eligibility before refund

Lisa saves the approved rule in the policy database. It is eligible for her
next scheduled QA cycle immediately; there is no separate activation step.

## 3. Run the sample agent

Copy and run the `docker compose ... refund-agent ...` command printed by the
start script. The agent uses Gemini to decide to issue a refund, and
Softprobe's Python instrumentation sends its live trace to TheLake. The sample
is deliberately written to call the refund tool without checking eligibility.

The command prints the session ID. If either provider reports that credits
are depleted, the trace may still be present, but a successful evaluator
verdict requires available OpenAI and Gemini credits. An evaluator `error` means
triage or judging could not complete; it is not a pass or a behavior failure.

## 4. Inspect Lisa's report

In Explorer, open the session ID printed by the agent. Inspect the trace and
the **Refund eligibility before refund** score. With working Gemini credits,
the expected result is `fail`, with an explanation that the agent issued the
refund without checking eligibility. Lisa's next scheduled cycle evaluates the
session, records the failure, and explains the evidence in her **Lisa scheduled
QA** conversation. Open that conversation in Explorer to read the report and
ask follow-up questions. Without the walkthrough override, the default hourly
cron runs at minute 0 UTC.

You can reply in the report conversation to ask follow-up questions.

## Automated evaluation check

The user walkthrough above is intentionally step by step. With the stack
running from step 1, the automated driver asks Lisa through her chat API to
propose and save an explicitly approved policy, runs the sample agent, verifies
the expected failure verdict, and checks that Lisa reports the evidence:

```bash
python3 tests/e2e/quickstart_online_eval.py
```

This driver currently expects the quickstart services to be running; it does
not start and stop the stack itself. The sample and judge require Gemini API
credits; policy triage also requires OpenAI API credits.

## Stop the quickstart

Press Ctrl+C in the first terminal, then stop its containers and remove its
local database volume:

```bash
docker compose --env-file .env --env-file warehouse/quickstart/compose.env \
  --project-name thelake-quickstart \
  --file examples/quickstart/compose.yaml down -v
```

## Next steps

- [Instrument your agent](how-to/instrumentation.md) to send its own traces.
- [Explore sessions and behavior checks](how-to/explorer.md).
- [Connect Slack](how-to/slack-evaluator.md) so Lisa can notify the originating
  thread when a scheduled evaluation finds an issue.
- [Behavior evaluation and Lisa's evidence limits](how-to/explorer.md#behavior-evaluation).
- [Policy memory](how-to/policy-memory.md) to maintain business rules as
  Markdown with explicit approval.
