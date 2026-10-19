INSERT INTO lisa.activity_monitor_message_threads(message_id, activity_id)
VALUES (%(external_id)s, %(activity_id)s)
ON CONFLICT DO NOTHING;
