INSERT INTO lisa.activity_monitor_runtime_work_threads(runtime_work_id, activity_id)
VALUES (%(external_id)s, %(activity_id)s)
ON CONFLICT DO NOTHING;
