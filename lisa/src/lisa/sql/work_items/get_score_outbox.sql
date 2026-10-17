SELECT id, work_item_id, status, score_payload, score_id
FROM lisa.score_outbox
WHERE work_item_id = %(work_item_id)s
ORDER BY created_at DESC
LIMIT 1;
