SELECT id, source_type, source_id, activity_id, created_at
FROM lisa.proactive_updates
ORDER BY created_at DESC, id
LIMIT %(limit)s;
