# Slack conversations with Lisa

For Lisa's canonical session, policy, and evaluation-pair terminology, see
[Lisa evaluation terms](../architecture/lisa-terms.md).

Slack is an optional front door to Lisa. Slack sends events directly to Lisa;
Lisa keeps the conversation, uses her policy and evaluation tools, reads trace
and check data from TheLake, and posts her own replies and issue alerts. TheLake
stores the telemetry and evaluation results. It does not receive Slack events
or start Lisa work.

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

The `LISA_THELAKE_API_URL` and `LISA_THELAKE_TOKEN` settings let Lisa read
workspace-scoped telemetry and write scores. The token is shared
with TheLake only to authenticate these Lisa-to-storage calls.

Set `LISA_SLACK_CHANNEL_ID` to choose a workspace-level destination for alerts
from checks that are not associated with a Slack thread. A check created from a
Slack conversation keeps that conversation's channel and thread as its alert
destination; those per-check bindings take precedence over the workspace
default. This default also allows checks created before Slack was configured to
send alerts without editing the policy text.

Set `LISA_SLACK_ACTIVITY_CHANNEL_ID` and optionally
`LISA_SLACK_ACTIVITY_THREAD_TS` to route Lisa's proactive activity updates into
one configured Lisa thread.

## Talk with Lisa

Mention the bot in a channel or reply in a supported thread. Lisa responds in
the same thread and keeps multi-turn context there. She can explain existing
policy and session trajectories, propose policy updates, and create behavior
policies. Lisa writes policies directly to `lisa.policies` using readable IDs
such as `P1.2`. Updates require the exact policy ID and change the same row. The
owner sends `Approve policy: <policy_id>` as a standalone message. Approved policy
is eligible for QA immediately.


Lisa schedules evaluation independently of Slack. Slack is a conversation and
notification surface; deleting or disconnecting it does not stop Lisa's
scheduled QA.

## Receive issue alerts

When Lisa's scheduled evaluation finds a failure, she stores the score in
TheLake and posts the finding to the Slack thread associated with the check, or
to `LISA_SLACK_CHANNEL_ID` when the check has no Slack binding.
Before a finding can be posted, OpenAI Decisions triages every approved
candidate policy and Gemini judges only suspected violations or unresolved
evidence. The alert includes the affected agent, session, policy, and judge
rationale; a Decisions `no_violation_found` result does not create a pass score.
Lisa keeps delivery receipts in her persistent workspace data so later
scheduled cycles suppress repeat alerts for the same policy, trace,
and Slack destination. A duplicate is still possible if Slack accepts a message
but Lisa stops before saving its receipt. Slack delivery is best effort;
Explorer and TheLake's score API remain the source of truth for recorded
results.

## Current limits

- Slack Events API is the supported inbound transport. Point its Request URL
  directly at Lisa; TheLake does not proxy Slack events.
- One Slack app installation maps to the workspace configured on that Lisa
  runtime.
- Failed evaluations remain in Lisa's persistent workspace follow-up queue
  until the notification succeeds or Lisa confirms there is no destination.
  The queue is local to the Lisa runtime and depends on retaining its data
  volume; it is not a distributed durable job queue.
