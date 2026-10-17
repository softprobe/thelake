SELECT id, work_item_id, status, score_payload, payload_hash, score_id
FROM lisa.score_outbox
WHERE idempotency_key = %(idempotency_key)s;
