---
name: configure-thelake-slack
description: Configure TheLake's Slack app, event endpoint, workspace binding, and natural-language behavior-check workflow.
---

# Configure TheLake Slack

Follow `docs/how-to/slack-evaluator.md` as the source of truth. A Slack app
administrator must create/install the app and grant its scopes; theLake
developers cannot bypass that customer-side requirement.
For Lisa's session and policy terminology, see
[`docs/architecture/lisa-terms.md`](../../../docs/architecture/lisa-terms.md).

## Configure

- Set the Events API request URL to `https://<thelake-host>/slack/events` and
  subscribe to `app_mention`. Add public or private thread event subscriptions
  and history scopes only if plain thread replies are required.
- Configure `chat:write`, `app_mentions:read`, and the appropriate history
  scopes from the guide. Invite the bot to the channels it needs to read.
- Provide signing secret and bot token to the service through its secret
  manager/environment. Set the Slack team ID and an explicit allowed user or
  channel list.
- Configure Lisa's workspace identity and Slack connection in Lisa's service.
  Slack connection state is not managed by TheLake's trace store.
- Configure the online evaluation runner and provider key if checks should
  evaluate incoming traces. Keep provider and Slack secrets out of docs,
  shell history, logs, and chat.

## Exercise it

In an approved test channel/thread, use the documented command:

```text
@Lisa Create an agent policy named "Refund eligibility" for support-agent:
check eligibility before issuing a refund.
```

Review the returned policy ID, then ask Lisa to approve that ID.


The target name must match the authenticated agent name on the root trace.
Owner-confirmed rules with `approved: yes` are eligible for evaluation
immediately. Run a safe sample agent, push its complete OTLP session to Lisa,
then check the Lisa evaluation activity and failure reply in the originating
thread separately. Failed notification attempts remain pending in Lisa's
workspace home for retry. Evaluation begins when Lisa durably accepts a pushed
session; there is no scheduled trace QA job. For a controlled end-to-end test,
use `verify-lisa-slack-alert`.

Do not send test messages to customer channels or use rules that trigger real
business actions.
