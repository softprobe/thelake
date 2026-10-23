WITH pending AS MATERIALIZED (
    SELECT message ->> 'activity_id' AS activity_id
    FROM pgmq.q_lisa_conversation_checkpoints
    WHERE message ->> 'kind' = 'activity_memory'
      AND message ->> 'activity_id' = %(activity_id)s
      AND message ->> 'event_watermark' = %(event_watermark)s
      AND coalesce((message ->> 'title_neutralized')::boolean, false)
          = %(title_neutralized)s::boolean
    LIMIT 1
), enqueued AS (
    SELECT pgmq.send('lisa_conversation_checkpoints', %(message)s::jsonb) AS message_id
    WHERE NOT EXISTS (SELECT 1 FROM pending)
)
SELECT message_id FROM enqueued;
