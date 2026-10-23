UPDATE lisa.activity_monitor_threads AS thread
SET details = CASE WHEN activity.title_neutralized THEN '{}'::jsonb
                   ELSE COALESCE(%(details)s::text::jsonb, thread.details) END,
    updated_at = now()
FROM lisa.activities AS activity
WHERE thread.activity_id = %(activity_id)s
  AND activity.id = thread.activity_id;
