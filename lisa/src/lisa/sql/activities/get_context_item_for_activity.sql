SELECT id, activity_id, seq, kind, content, content_hash
FROM lisa.activity_context
WHERE activity_id = %(activity_id)s
  AND kind = %(kind)s
ORDER BY seq DESC
LIMIT 1;
