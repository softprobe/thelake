# Slack behavior checks

TheLake can create and activate a natural-language behavior check from a Slack
thread, then post failures from online evaluation back into that thread. The
thread destination is stored with the evaluator definition; evaluation scores
remain in the workspace's normal score store.

## Configure a Slack app

Create a Slack app for your installation and configure:

- Bot token scope: `chat:write`
- Event subscription: `app_mention`
- Event subscription: `message.channels` for plain replies in public-channel
  threads
- Add `message.groups` and the `groups:history` scope to support private-channel
  threads; invite the bot to the channel
- Bot scopes: `app_mentions:read`, `channels:history` (for plain thread replies),
  and `groups:history` when private channels are enabled
- Event Request URL: `https://<thelake-host>/slack/events`

Set these environment variables on theLake service:

```bash
THELAKE_SLACK_SIGNING_SECRET=<Slack app signing secret>
THELAKE_SLACK_BOT_TOKEN=<Slack bot token>
THELAKE_SLACK_TEAM_ID=<Slack workspace ID where the app is installed>
THELAKE_SLACK_ALLOWED_USERS=<comma-separated Slack user IDs>
# Or restrict authoring to one or more channels instead:
THELAKE_SLACK_ALLOWED_CHANNELS=<comma-separated Slack channel IDs>
```

At least one of the author allowlists must contain an ID. A request is
authorized when its user ID **or** its channel ID is allowlisted.

In isolated workspace mode, also set `THELAKE_SLACK_WORKSPACE_ID` to the
workspace that owns the checks. If it is unset, the integration uses
`THELAKE_DEFAULT_WORKSPACE_ID`, then the shared workspace. Provision isolated
workspaces before using Slack. This installation currently binds one Slack app
to one configured theLake workspace.

For online evaluation, configure and run the evaluation runner as described in
the [Explorer behavior checks guide](explorer.md#behavior-checks):
`THELAKE_EVALUATION_RUNNER_URL` and `THELAKE_EVALUATION_RUNNER_TOKEN` must be
set on theLake service, and the runner needs its model-provider credentials.

## Create a check from Slack

In a channel thread, mention the bot using this form:

```text
@thelake evaluate support-agent :: Before issuing a refund, check the ticket's eligibility and explain the result.
```

The agent name must match the authenticated agent name on the root trace. The
criteria are evaluated by the configured online judge against each completed
trace. TheLake creates and activates the check and replies in the same thread.
Plain thread replies work with the same command when the bot has the history
scopes above. Other conversation text is ignored.

Each evaluator failure is stored as a score and then posted to the originating
thread with the check name, agent name, trace ID, and judge rationale. Slack
delivery is best effort; Explorer and the score API remain the source of truth
for evaluation results.

The Events API response is sent after the evaluator is saved and activated. If
Slack retries a failed or timed-out event, its event ID maps to the same
evaluator, so the retry won't create a second check. If Slack accepted the
thread reply but the response was lost immediately before the durable receipt
was completed, a later retry can repeat that reply. Event receipts are kept in
the catalog Postgres registry and completed receipts are pruned after 30 days.

## Current boundaries

- A Slack app is bound to one configured theLake workspace. Use separate app
  installations or deployments to connect separate workspaces.
- Slack authoring currently uses the `evaluate <agent> :: <criteria>` command.
  Evaluator editing and pause/resume remain available in Explorer or the API.
- Failed-score notifications are sent after score persistence. Delivery is not
  queued for retry if Slack is unavailable.
