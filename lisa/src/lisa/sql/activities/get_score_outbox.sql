SELECT id, evaluation_id, status, score_payload, score_id
FROM lisa.score_outbox
WHERE evaluation_id = %(evaluation_id)s
ORDER BY created_at DESC
LIMIT 1;
