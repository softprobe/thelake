# 5-minute quickstart

Walk through TheLake's online evaluation yourself: start the local stack, create
a natural-language behavior check, run a deliberately flawed support agent,
then inspect its trace and evaluation in Explorer and ask Lisa to investigate.
Initial builds and image downloads can take longer than five minutes.

The sample agent issues a refund without checking eligibility. Its refund tool
returns a local demo result and is not connected to a payment system.

## Before you start

Install Docker with Compose, Rust, Node.js 24 LTS with npm, Python 3, `make`, and
`curl`. Put a Gemini API key with available Gemini API credits in the gitignored
`.env` file as `GEMINI_API_KEY=...`, or enter it at the hidden prompt when the
start script asks. The sample agent and Lisa's local evaluator make live Gemini
API calls.

## 1. Start TheLake

From the repository root, run:

```bash
LISA_QA_CRON="*/5 * * * *" bash examples/quickstart/start.sh
```

The script generates a local Lisa service token, starts Postgres and Lisa, then
runs TheLake in the foreground. This five-minute cron is for the walkthrough;
deployments default to hourly. Lisa runs the DeepEval judge in her MCP tool
process. It prints the Explorer URL and the next commands. Keep this terminal
open and open the printed URL.

## 2. Create the behavior check

In Explorer, open Lisa's chat and send:

> For `quickstart-refund-agent`, check refund eligibility and wait for the result before issuing a refund. Start monitoring now.

Lisa creates the behavior check from your request, activates it because you
explicitly asked her to start monitoring, and confirms the active version.
That check is available to Lisa's scheduled QA cycle.

## 3. Run the sample agent

Copy and run the `docker compose ... refund-agent ...` command printed by the
start script. The agent uses Gemini to decide to issue a refund, and
Softprobe's Python instrumentation sends its live trace to TheLake. The sample
is deliberately written to call the refund tool without checking eligibility.

The command prints the session ID. If Gemini reports that prepayment credits
are depleted, the trace may still be present, but a successful evaluator
verdict requires available Gemini credits. An evaluator `error` means the rule
could not be judged; it is not a pass or a behavior failure.

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
create and activate a natural-language check, runs the sample agent, verifies
the expected failure verdict, and checks that Lisa reports the evidence:

```bash
python3 tests/e2e/quickstart_online_eval.py
```

This driver currently expects the quickstart services to be running; it does
not start and stop the stack itself. The sample and judge both require Gemini
API credits.

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
  Markdown and submit reviewable evaluator drafts from a coding agent.
