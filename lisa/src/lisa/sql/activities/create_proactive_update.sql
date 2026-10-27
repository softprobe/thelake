INSERT INTO lisa.proactive_updates(id, source_type, source_id, activity_id)
VALUES (%(id)s, %(source_type)s, %(source_id)s, %(activity_id)s)
ON CONFLICT (source_type, source_id) DO NOTHING
RETURNING id, source_type, source_id, activity_id, created_at;
