INSERT INTO lisa.activity_monitor_runtime_event_threads(runtime_event_id, activity_id)
VALUES (%(external_id)s, %(activity_id)s)
ON CONFLICT DO NOTHING;
