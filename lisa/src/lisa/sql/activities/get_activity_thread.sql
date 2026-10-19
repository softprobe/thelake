SELECT a.id, a.kind, a.status, a.title, a.summary, a.source,
       a.created_at, a.started_at, a.updated_at, a.finished_at,
       f.activity_type, f.is_goal, f.message_id, f.subtitle, f.status_title,
       f.task_label, f.agent_id, f.details AS details_json,
       t.icon, t.details AS thread_details
FROM lisa.activities AS a
JOIN lisa.activity_feed_entries AS f ON f.activity_id = a.id
JOIN lisa.activity_monitor_threads AS t ON t.activity_id = a.id
WHERE a.id = %(activity_id)s;
