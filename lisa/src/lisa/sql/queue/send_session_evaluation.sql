SELECT pgmq.send('lisa_session_evaluations', %(message)s::jsonb) AS message_id;
