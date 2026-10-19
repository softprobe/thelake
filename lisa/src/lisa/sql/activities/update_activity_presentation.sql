UPDATE lisa.activity_feed_entries
SET subtitle = COALESCE(%(subtitle)s::text, subtitle),
    status_title = COALESCE(%(status_title)s::text, status_title),
    task_label = COALESCE(%(task_label)s::text, task_label),
    details = CASE WHEN %(details)s::text IS NULL THEN details ELSE %(details)s::jsonb END,
    updated_at = now()
WHERE activity_id = %(activity_id)s;
