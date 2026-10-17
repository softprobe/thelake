SELECT id, work_item_id, seq, kind, content, content_hash
FROM lisa.work_context
WHERE work_item_id = %(work_item_id)s
  AND kind = %(kind)s
ORDER BY seq DESC
LIMIT 1;
