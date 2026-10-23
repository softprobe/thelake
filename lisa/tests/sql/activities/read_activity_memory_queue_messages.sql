SELECT message
FROM pgmq.q_lisa_conversation_checkpoints
WHERE message ->> 'kind' = 'activity_memory'
  AND message ->> 'activity_id' = %s
ORDER BY msg_id;
