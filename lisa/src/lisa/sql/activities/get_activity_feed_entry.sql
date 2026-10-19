SELECT a.id, a.kind, a.parent_id, a.status, a.title, a.summary, a.source,
       a.created_at, a.started_at, a.updated_at, a.finished_at,
       a.next_action, a.next_action_at, a.origin_conversation_id,
       a.origin_slack_channel_id, a.origin_slack_thread_ts,
       f.activity_type, f.is_goal, f.subtitle, f.status_title, f.task_label,
       f.agent_id, f.details AS details_json
FROM lisa.activities AS a
JOIN lisa.activity_feed_entries AS f ON f.activity_id = a.id
WHERE a.id = %(activity_id)s;
