SELECT id, work_item_id, seq, kind, content, content_hash
FROM lisa.work_context
WHERE id = %(id)s;
