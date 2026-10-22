WITH seeded_outbox AS (
    INSERT INTO lisa.score_outbox(
        id, evaluation_id, idempotency_key, score_payload, payload_hash,
        status, attempt, score_id, acknowledged_at
    )
    VALUES (
        %(outbox_id)s, %(evaluation_id)s, %(score_id)s,
        %(score_payload)s::jsonb, %(payload_hash)s,
        'acknowledged', 1, %(score_id)s, clock_timestamp()
    )
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING id, evaluation_id
), completed AS (
    UPDATE lisa.evaluation_runs
    SET status = 'succeeded',
        verdict = %(verdict)s,
        score_id = %(score_id)s,
        started_at = clock_timestamp(),
        finished_at = clock_timestamp()
    WHERE id = %(evaluation_id)s
      AND EXISTS (SELECT 1 FROM seeded_outbox WHERE evaluation_id = %(evaluation_id)s)
    RETURNING id, score_id
)
SELECT seeded_outbox.id
FROM seeded_outbox
JOIN completed ON completed.id = seeded_outbox.evaluation_id;
