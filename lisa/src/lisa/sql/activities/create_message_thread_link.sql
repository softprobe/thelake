INSERT INTO lisa.activity_monitor_message_threads(message_id, activity_id)
SELECT %(message_id)s::text, %(activity_id)s::uuid WHERE %(message_id)s::text IS NOT NULL
ON CONFLICT DO NOTHING;
