---
name: verify-lisa-slack-alert
description: Verify that Lisa proactively evaluates a fresh agent trace and posts a verified failure through the workspace's single Slack connection.
---

# Verify Lisa's Slack alert

Lisa owns the Slack connection for her workspace. The bot token authenticates
that one connection; `LISA_SLACK_CHANNEL_ID` is the workspace's default alert
channel. A check created from a Slack thread may keep that thread as its
destination. A check without a Slack binding uses the workspace default.
TheLake stores traces and scores; it does not run or notify Lisa.

## Preconditions

1. Run only when the user explicitly requests a live Slack test. It sends a
   message to Slack and makes paid model calls.
2. Use a dedicated test workspace and channel, and a sample agent with
   local-only tools. Do not use a customer or production channel.
3. Confirm the Lisa service has its bot token and workspace default channel
   configured. Check configuration status only; never print or copy secret
   values.
4. Confirm TheLake and Lisa are healthy and Lisa's TheLake API URL points to
   the running TheLake service. Do not proceed if trace reads
   fail due to a disconnected catalog.
5. Read `docs/how-to/slack-evaluator.md` for current Slack setup and routing.

## Run the autonomous check

1. Record the current scheduled QA conversation timestamp using a read-only
   request. Do not prompt Lisa or call her scheduled-cycle endpoint.
2. Ensure an active behavior check applies to the sample agent. Prefer a check
   without `slack_channel_id` to verify the workspace default route.
3. Run the deliberately flawed quickstart refund agent and record its session
   ID. It only simulates a refund tool; it does not contact a payment system.
4. Wait for Lisa's normal schedule. Verify that she selects the new session,
   stores a failing evaluator score, and reports the evidence in her scheduled
   QA conversation.
5. Verify Slack separately: the alert should appear in the configured default
   channel, include the new session, and name the failed check. If a check has
   its own Slack channel and thread binding, verify it in that thread instead.
6. Let Lisa evaluate and notify through her scheduled workflow. Do not call the
   evaluator or notification tools directly and then describe that as
   proactive behavior.

## Report

Report trace evaluation, Lisa's scheduled report, and Slack delivery as
separate outcomes. Include the session and score IDs, check name, destination
type (workspace default or check thread), and whether the Slack message was
found. Never include API keys, bot tokens, or full trace payloads.

Slack delivery is best effort. Persistent receipts suppress ordinary repeated
alerts for the same policy, trace, and destination. A duplicate is still
possible if Slack accepts a message but Lisa stops before saving the receipt.
