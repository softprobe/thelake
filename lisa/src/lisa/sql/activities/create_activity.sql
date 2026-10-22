INSERT INTO lisa.activities(
    id, kind, parent_id, status, title, source, context_path,
    next_action, next_action_at, origin_conversation_id,
    origin_slack_channel_id, origin_slack_thread_ts,
    slack_channel_id, slack_thread_ts
)
VALUES (
    %(id)s, %(kind)s, %(parent_id)s, 'queued', %(title)s, %(source)s, %(context_path)s,
    %(next_action)s, %(next_action_at)s, %(origin_conversation_id)s,
    %(origin_slack_channel_id)s, %(origin_slack_thread_ts)s,
    %(slack_channel_id)s, %(slack_thread_ts)s
)
ON CONFLICT (id) DO NOTHING
RETURNING id;
