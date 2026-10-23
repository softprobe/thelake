SELECT a.id, a.kind, a.parent_id, a.status, a.title, a.summary, a.source,
       a.created_at, a.started_at, a.updated_at, a.finished_at,
       a.next_action, a.next_action_at, a.origin_conversation_id,
       a.origin_slack_channel_id, a.origin_slack_thread_ts,
       f.activity_type, f.is_goal, f.message_id, f.subtitle, f.status_title,
       f.task_label, f.agent_id, f.details AS details_json
FROM lisa.activities AS a
JOIN lisa.activity_feed_entries AS f ON f.activity_id = a.id
WHERE (%(is_goal)s::boolean IS NULL OR f.is_goal = %(is_goal)s::boolean)
  AND (%(activity_type)s::text IS NULL OR f.activity_type = %(activity_type)s::text)
  AND (%(status)s::text IS NULL OR a.status = %(status)s::text)
  AND (NOT %(open_only)s::boolean OR a.status IN ('queued', 'running', 'waiting', 'waiting_approval'))
  AND (%(query)s::text IS NULL OR (a.title ILIKE '%%' || %(query)s::text || '%%'
       OR COALESCE(a.summary, '') ILIKE '%%' || %(query)s::text || '%%'
       OR COALESCE(f.subtitle, '') ILIKE '%%' || %(query)s::text || '%%'
       OR f.search_title ILIKE '%%' || %(query)s::text || '%%'))
  AND (NOT %(due_only)s::boolean OR (a.next_action_at IS NOT NULL AND a.next_action_at <= now()))
ORDER BY COALESCE(a.finished_at, a.updated_at, a.created_at) DESC
LIMIT %(limit)s;
