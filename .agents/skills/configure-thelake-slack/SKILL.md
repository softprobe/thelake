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
@Lisa Propose an agent POLICY.md rule named "Refund eligibility" for support-agent:
check eligibility before issuing a refund. Show approved: yes, but do not save yet.
```

Review the section, then send `@Lisa Confirm policy rule: Refund eligibility`.


The target name must match the authenticated agent name on the root trace.
Owner-confirmed rules with `approved: yes` are eligible for evaluation immediately. Run a safe sample agent, then check
score persistence in Explorer/API and the failure reply in the originating
thread separately. Failed notification attempts remain pending in Lisa's workspace home for retry. Lisa evaluates on her `Lisa scheduled QA` cron schedule, hourly at
minute 0 UTC by default; she does not run on startup. For a faster controlled
test, configure a short `LISA_QA_CRON` before starting Lisa, then use
`verify-lisa-slack-alert`.

Do not send test messages to customer channels or use rules that trigger real
business actions.
