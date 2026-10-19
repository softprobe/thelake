SELECT id, activity_id, seq, kind, content, content_hash
FROM lisa.activity_context
WHERE id = %(id)s;
