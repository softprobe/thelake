INSERT INTO lisa.score_outbox(
    id, work_item_id, idempotency_key, score_payload, payload_hash
)
VALUES (
    %(id)s, %(work_item_id)s, %(idempotency_key)s,
    %(score_payload)s::jsonb, %(payload_hash)s
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id, work_item_id, status, score_payload, payload_hash, score_id;
