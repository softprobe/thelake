WITH conversation_lock AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(hashtextextended(%(conversation_id)s, 0))
), pending AS MATERIALIZED (
    SELECT message ->> 'request_id' AS request_id
    FROM pgmq.q_lisa_conversation_checkpoints, conversation_lock
    WHERE message ->> 'conversation_id' = %(conversation_id)s
    ORDER BY msg_id
    LIMIT 1
), enqueued AS (
    SELECT pgmq.send('lisa_conversation_checkpoints', %(message)s::jsonb) AS message_id
    WHERE NOT EXISTS (SELECT 1 FROM pending)
)
SELECT
    (SELECT message_id FROM enqueued) AS message_id,
    (SELECT request_id FROM pending) AS pending_request_id;
