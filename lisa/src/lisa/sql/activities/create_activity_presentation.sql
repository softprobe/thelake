INSERT INTO lisa.activity_feed_entries
    (activity_id, activity_type, is_goal, message_id, subtitle, status_title, task_label, agent_id, details)
VALUES
    (%(activity_id)s, %(activity_type)s, %(is_goal)s, %(message_id)s,
     %(subtitle)s, %(status_title)s, %(task_label)s, %(agent_id)s, %(details)s::jsonb)
ON CONFLICT (activity_id) DO UPDATE SET
    activity_type = EXCLUDED.activity_type,
    is_goal = EXCLUDED.is_goal,
    message_id = EXCLUDED.message_id,
    subtitle = EXCLUDED.subtitle,
    status_title = EXCLUDED.status_title,
    task_label = EXCLUDED.task_label,
    agent_id = EXCLUDED.agent_id,
    details = EXCLUDED.details,
    updated_at = now();
