UPDATE lisa.activity_monitor_threads
SET details = COALESCE(%(details)s::text::jsonb, details), updated_at = now()
WHERE activity_id = %(activity_id)s;
