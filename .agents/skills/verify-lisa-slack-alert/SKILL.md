---
name: verify-lisa-slack-alert
description: Verify that a failing online evaluator score is posted to its originating Slack thread. Use when a developer wants to test TheLake's configured Slack evaluator integration.
---

# Verify Lisa Slack alert

This skill verifies the optional Slack notification path for TheLake evaluator
failures. It does not configure a Slack app or create a Lisa workspace agent.
The integration binds one configured Slack app to one TheLake workspace, and
notification delivery is best effort after the failure score is stored.

## Preconditions

1. Run only when the user explicitly requests a Slack notification test. It
   sends a message to Slack.
2. Use a dedicated test channel and thread, a test workspace, and a sample
   agent with local-only tools. Do not test on customer or production channels.
3. Confirm the TheLake service has the required Slack environment variables
   configured. Check variable names or service configuration status only;
   never print or copy token/signing-secret values.
4. Confirm the online evaluation runner is configured and has its model key.
   Stop and report missing configuration rather than searching files for
   credentials.
5. Read `docs/how-to/slack-evaluator.md` for the current Slack scopes,
   allowlist, event URL, and workspace binding.

## Run the notification check

1. In the dedicated test thread, use the documented Slack command form:

   ```text
   @thelake evaluate <test-agent-name> :: <a clear behavior rule that the test agent will violate>
   ```

   Slack-created checks activate immediately. Keep the rule harmless and
   observable; do not make it direct a real refund, booking, or other external
   action. Note the exact agent name and originating thread.
2. Run the test agent and confirm it sends a completed trace to the configured
   TheLake workspace. Reuse the sample agent and connection setup in the
   `run-lisa-evaluation-dogfood` skill where appropriate. The trace root's
   authenticated agent name must exactly match the evaluator target. Avoid an
   agent name that already has a sampled trace inside the current 60-second
   interval, or wait for the interval to expire; otherwise the notification
   path may not run for this test trace.
3. Verify the evaluator failure score is persisted and references the trace.
   Explorer or the session API is the source of truth for this step.
4. Verify a bot reply appears in the same Slack thread and includes the
   evaluator failure details, trace ID, and rationale. Confirm it is linked to
   the same score/trace you inspected.

## Report the result

Report score persistence and Slack delivery as separate outcomes:

- **Score missing:** investigate trace ingest, sampler eligibility, evaluator
  activation/agent-name match, and runner outcome before diagnosing Slack.
- **Score present, Slack reply missing:** report notification delivery as
  failed. It is best effort and is not retried after a delivery failure.
- **Both present:** record the evaluator ID, trace ID, score ID, test channel,
  and thread timestamp. Do not include Slack tokens or customer payloads.

Do not claim the broader policy-learning/review flow was tested; this check
covers only the Slack command-to-online-evaluation notification path.
