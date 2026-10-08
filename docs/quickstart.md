# 5-minute quickstart

Send a sample agent trace, describe the behavior you expect, and see the
evaluation result attached to that trace. This walkthrough uses a synthetic
refund conversation, so it does not need access to your agent's codebase.

The quickstart runs theLake and Postgres locally. The evaluation runner sends
captured prompt, response, and tool evidence to Gemini. Set a Gemini API key
before you activate a check. The Explorer asks for consent first; common
credential patterns are redacted, but general personal data is not.

## Before you start

- Docker with Compose, Rust, Node.js with npm, Python 3, `make`, and `curl`
- A Gemini API key in `GOOGLE_API_KEY` or `GEMINI_API_KEY`

The first build and runner image download can take longer than five minutes.
The timed walkthrough starts after those are ready. The start script chooses
free localhost ports from a small range and prints the Explorer URL. You can
pin ports with `THELAKE_QUICKSTART_DB_PORT`,
`THELAKE_QUICKSTART_RUNNER_PORT`, and `THELAKE_QUICKSTART_PORT`.

## Start the local stack

From the repository root, start the quickstart. It starts an isolated local
Postgres database and evaluator runner, then launches theLake in the
foreground:

```bash
export GOOGLE_API_KEY="your-gemini-api-key"
bash examples/quickstart/start.sh
```

The script generates a local runner token for this session and binds theLake
and runner to localhost. It uses a dedicated quickstart config and local data
directory; it does not change your normal `config.yaml`.

Open the Explorer URL printed in the first terminal, choose **Behavior
checks**, and save this check:

| Field | Value |
|---|---|
| Agent name | `quickstart-refund-agent` |
| Check name | `Check eligibility before refund` |
| Expected behavior | `Before issuing a refund, verify the ticket is eligible. If eligibility has not been checked, do not issue the refund.` |

Confirm the Gemini data notice and choose **Save and activate**. The check
applies to new matching traces.

## Send the sample trace

In another terminal, run:

```bash
python3 examples/quickstart/send_sample_trace.py
```

This sends a synthetic conversation where the agent issues a refund without
checking eligibility. The command prints the session ID. Refresh Explorer,
choose **Sessions**, search for that ID, open the session, then select the
agent span. After a few seconds, **Evaluation results** should show a `fail`
result with the judge's explanation and trace evidence.

That is the core loop: theLake receives OpenTelemetry traces, applies your
plain-language behavior check to new traces, and stores the result with the
trace so you can inspect what happened.

## Next steps

- [Instrument your agent](how-to/instrumentation.md) to send its real traces.
- [Explore sessions and behavior checks](how-to/explorer.md) for local setup,
  auth, and the full UI reference.
- [Connect Slack](how-to/slack-evaluator.md) to author checks and receive
  failure notifications in a thread.
- [Evaluation runner details](../evaluation-runner/README.md), including
  evidence limits and provider behavior.

Press Ctrl+C in the first terminal to stop theLake. Stop the quickstart
containers with:

```bash
docker compose --project-name thelake-quickstart \
  --file examples/quickstart/compose.yaml down -v
```
