---
name: verify-lisa-slack-alert
description: Verify that Lisa evaluates a pushed agent session and posts a verified failure through its Slack connection.
---

# Verify Lisa's Slack alert

Use [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md) for the
session, policy, and evaluation-pair identifiers reported by this workflow.

Lisa owns the Slack connection for her configured workspace. A producer pushes
session traces to Lisa's private-network `/internal/v1/session-traces` endpoint;
Lisa stores evidence locally and evaluates it from her PGMQ queue. TheLake is
not in the evaluation or notification path.

## Preconditions

1. Run only when the user explicitly requests a live Slack test. It sends a
   message to Slack and makes paid model calls.
2. Use a dedicated test workspace and channel, and a sample agent with
   local-only tools. Do not use a customer or production channel.
3. Confirm Lisa's service has its bot token and default alert channel
   configured. Check status only; never print or copy secret values.
4. Read `docs/how-to/slack-evaluator.md` for current Slack setup and routing.

## Run the push-triggered check

1. Ensure an approved behavior policy applies to the sample agent. Prefer a
   policy without a channel override to verify the workspace default route.
2. Run the deliberately flawed dogfood agent and capture its complete OTLP
   session. It only simulates a refund tool; it does not contact a payment
   system.
3. Push the OTLP JSON to Lisa over its private network intake. Confirm the API
   accepts it, then wait for its evaluation activity to reach completion.
4. Verify the stored result is failing and uses evidence from the pushed
   session. Verify Slack separately: it should include the session and failed
   policy name in the configured default channel or policy thread.
5. Do not call evaluator or notification tools directly and then describe that
   as a successful push-triggered path.

## Report

Report push receipt, evaluation result, and Slack delivery as separate
outcomes. Include the session ID, policy name, destination type (workspace
default or policy thread), and whether the Slack message was found. Never
include API keys, bot tokens, or full trace payloads.

Slack delivery is best effort. Persistent receipts suppress ordinary repeated
alerts for the same policy, session, and destination. A duplicate is possible
if Slack accepts a message but Lisa stops before saving the receipt.
