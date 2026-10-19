SELECT a.id, a.kind, a.parent_id, a.status, a.title, a.summary, a.source,
       a.created_at, a.started_at, a.updated_at, a.finished_at,
       f.activity_type, f.is_goal, f.message_id, f.subtitle, f.status_title,
       f.task_label, f.agent_id, f.details AS details_json
FROM lisa.activities AS a
JOIN lisa.activity_feed_entries AS f ON f.activity_id = a.id
WHERE (%(is_goal)s::boolean IS NULL OR f.is_goal = %(is_goal)s::boolean)
  AND (%(activity_type)s::text IS NULL OR f.activity_type = %(activity_type)s::text)
ORDER BY COALESCE(a.finished_at, a.updated_at, a.created_at) DESC
LIMIT %(limit)s;
