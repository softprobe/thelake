SELECT pgmq.create('lisa_conversation_checkpoints')
WHERE NOT EXISTS (
    SELECT 1 FROM pgmq.list_queues()
    WHERE queue_name = 'lisa_conversation_checkpoints'
);
