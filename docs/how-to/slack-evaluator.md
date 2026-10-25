# Slack conversations with Lisa

Slack is an optional front door and notification channel for Lisa. Lisa's
Python service handles Slack events, serves the Lisa web UI, and runs the
policy evaluation worker. Lisa stores workspace policies, activities, and
evaluation records in its own PostgreSQL database. TheLake is not required at
runtime.

## Configure Lisa's Slack connection

Create a Slack app for the deployment and configure:

- Bot token scope: `chat:write`
- Event subscriptions: `app_mention` and `message.channels`
- For private channels, also subscribe to `message.groups`, request the
  `groups:history` scope, and invite the bot to the channel
- Bot scopes: `app_mentions:read`, `channels:history` for plain public-channel
  thread replies, and `groups:history` for private-channel replies
- Event Request URL: `https://<lisa-host>/slack/events`

Set these variables on Lisa's service:

```bash
LISA_SLACK_SIGNING_SECRET=<Slack app signing secret>
LISA_SLACK_BOT_TOKEN=<Slack bot token>
LISA_SLACK_TEAM_ID=<Slack workspace ID where the app is installed>
LISA_SLACK_ALLOWED_USERS=<comma-separated Slack user IDs>
# Or restrict access to one or more channels instead:
LISA_SLACK_ALLOWED_CHANNELS=<comma-separated Slack channel IDs>
```

At least one allowlist must be configured. A request is authorized when its
user ID or channel ID is allowlisted. `LISA_WORKSPACE_ID` scopes this Lisa
runtime to its workspace. Run one Lisa runtime per workspace and use a separate
Slack app or deployment for each workspace.

Set `LISA_SLACK_CHANNEL_ID` to choose the workspace-level alert destination.
A failure associated with a Slack conversation keeps that conversation's
channel and thread as its alert destination; those bindings take precedence.

Set `LISA_SLACK_ACTIVITY_CHANNEL_ID` and optionally
`LISA_SLACK_ACTIVITY_THREAD_TS` as the Slack destination for an evaluation
failure that has no conversation-specific channel.

## Session evaluation and alerts

Lisa evaluates sessions pushed to its unauthenticated
`POST /internal/v1/session-traces` endpoint, which must remain on a private
network. The producer sends one complete OTLP JSON bundle; there is no bearer
credential on this endpoint. Lisa validates and atomically writes the session
bundle locally, then commits the receipt and durable PGMQ job together in
Lisa's PostgreSQL database. This Slack evaluation path uses the pushed bundle;
the optional Lisa-owned TheLake sampler is not involved. A successful response confirms receipt and queue insertion,
not evaluation completion or a cross-store transaction.

OpenAI Decisions triages approved candidate policies. Gemini judges suspected
violations and insufficient evidence. Lisa saves policy snapshots, triage
decisions, evaluation results, and activities in its own database. A reviewed
failure can be sent to Slack. Successful notification receipts suppress
duplicate sends for the same policy, trace, and destination; Slack delivery is
best effort if the process stops between a successful post and receipt save.

## Current limits

- Slack Events API is the supported inbound transport. Point its Request URL
  directly at Lisa; TheLake does not proxy Slack events.
- One Slack app installation maps to the workspace configured on that Lisa
  runtime.
- Lisa can evaluate only sessions successfully pushed to its evidence store.
  Producers must retry failed submissions and monitor delivery results.
- The unauthenticated session intake must remain on a private network.
