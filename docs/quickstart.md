# 5-minute quickstart

Describe a behavior you want to catch, run a real Gemini-powered sample agent,
and see the evaluation result attached to its trace. The included refund agent
is deliberately flawed so the check can demonstrate a real detected issue.
The walkthrough uses Explorer's browser-based behavior-check wizard; it does
not require a Slack workspace or Slack admin access. The UI labels this area
**Chat**, but it is not a conversational AI agent: its assistant messages are
fixed prompts for entering criteria and an agent name. Gemini is used later by
the evaluation runner to judge traces after the sample agent runs.

The quickstart runs theLake and Postgres locally. The evaluation runner sends
captured prompt, response, and tool evidence to Gemini. Set a Gemini API key
before you activate a check. The Explorer asks for consent first; common
credential patterns are redacted, but general personal data is not.

## Before you start

- Docker with Compose, Rust, Node.js with npm, Python 3, `make`, and `curl`
- A Gemini API key in `GOOGLE_API_KEY` or `GEMINI_API_KEY`

The first builds and image downloads for the runner and sample agent can take
longer than five minutes.
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

Open the Explorer URL printed in the first terminal. In **Chat**, enter the
behavior-check criteria when the wizard prompts you:

> Before issuing a refund, verify the ticket is eligible and explain the result.

When the wizard prompts for an agent, enter `quickstart-refund-agent`.
Review the check, confirm the Gemini data notice, and choose **Activate check**.
The check applies to new matching traces.

## Run the sample agent

In another terminal, export the same Gemini key and run the command printed by
the first terminal. The command loads the generated runner token from the
local-only `warehouse/quickstart/compose.env` file, so it does not need to be
copied between terminals. It uses the right local-network address for your
platform, calls Gemini with a real tool declaration, invokes the refund tool,
and exports the resulting OTLP spans to your local theLake.
The sample uses Softprobe's Python auto-instrumentation wrapper through
Gemini's OpenAI-compatible endpoint; only the application-owned refund tool
execution is instrumented explicitly.
The demo agent makes live Gemini calls but its refund tool only returns a local
demo result; it does not connect to a ticketing or payment system.

The command prints the session ID. Explorer's monitoring view polls for recent
evaluation results; it should show a `fail` result with the judge's explanation
and links to the session and trace evidence. The composer does not call Gemini
or interpret follow-up questions.

That is the core loop: a live agent sends OpenTelemetry traces, theLake applies
your plain-language behavior check through the online evaluation runner, and
Explorer displays the trace-linked result. The wizard creates the check; it
does not act as a chat agent.

## Next steps

- [Instrument your agent](how-to/instrumentation.md) to send its real traces.
- [Explore sessions and behavior checks](how-to/explorer.md) for local setup,
  auth, and the full UI reference.
- [Connect Slack](how-to/slack-evaluator.md) to author checks and receive
  failure notifications in a thread.
- [Evaluation runner details](../evaluation-runner/README.md), including
  evidence limits and provider behavior.
- [Policy memory](how-to/policy-memory.md) to maintain business rules as
  Markdown and submit reviewable evaluator drafts from a coding agent.

Press Ctrl+C in the first terminal to stop theLake. Stop the quickstart
containers with:

```bash
docker compose --env-file warehouse/quickstart/compose.env \
  --project-name thelake-quickstart \
  --file examples/quickstart/compose.yaml down -v
```
