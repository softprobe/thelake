---
name: configure-thelake-slack
description: Configure TheLake's Slack app, event endpoint, workspace binding, and natural-language behavior-check workflow.
---

# Configure TheLake Slack

Follow `docs/how-to/slack-evaluator.md` as the source of truth. A Slack app
administrator must create/install the app and grant its scopes; theLake
developers cannot bypass that customer-side requirement.

## Configure

- Set the Events API request URL to `https://<thelake-host>/slack/events` and
  subscribe to `app_mention`. Add public or private thread event subscriptions
  and history scopes only if plain thread replies are required.
- Configure `chat:write`, `app_mentions:read`, and the appropriate history
  scopes from the guide. Invite the bot to the channels it needs to read.
- Provide signing secret and bot token to the service through its secret
  manager/environment. Set the Slack team ID and an explicit allowed user or
  channel list.
- Bind the app to the intended TheLake workspace. The current integration
  binds one app to one configured workspace; isolated workspaces must already
  be provisioned.
- Configure the online evaluation runner and provider key if checks should
  evaluate incoming traces. Keep provider and Slack secrets out of docs,
  shell history, logs, and chat.

## Exercise it

In an approved test channel/thread, use the documented command:

```text
@thelake evaluate support-agent :: Before issuing a refund, check eligibility and explain the result.
```

The target name must match the authenticated agent name on the root trace.
Slack-created checks activate immediately. Run a safe sample agent, then check
score persistence in Explorer/API and the failure reply in the originating
thread separately. Failure notifications are best effort and are not queued for
retry. Automatic evaluation admits the first eligible trace per workspace and
authenticated agent every 60 seconds per service process by default. Wait for
that interval if the agent had a recent trace, or the sample may not trigger an
evaluation. For a controlled test, use `verify-lisa-slack-alert`.

Do not send test messages to customer channels or use rules that trigger real
business actions.
