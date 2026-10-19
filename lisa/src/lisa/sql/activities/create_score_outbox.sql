INSERT INTO lisa.score_outbox(
    id, evaluation_id, idempotency_key, score_payload, payload_hash
)
VALUES (
    %(id)s, %(evaluation_id)s, %(idempotency_key)s,
    %(score_payload)s::jsonb, %(payload_hash)s
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id, evaluation_id, status, score_payload, payload_hash, score_id;
