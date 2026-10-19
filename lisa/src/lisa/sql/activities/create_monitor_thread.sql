INSERT INTO lisa.activity_monitor_threads(activity_id, details)
VALUES (%(activity_id)s, %(details)s::jsonb)
ON CONFLICT (activity_id) DO UPDATE SET details = EXCLUDED.details, updated_at = now();
