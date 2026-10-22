SELECT pgmq.create('lisa_session_evaluations')
WHERE NOT EXISTS (
    SELECT 1 FROM pgmq.list_queues()
    WHERE queue_name = 'lisa_session_evaluations'
);
CREATE TABLE IF NOT EXISTS lisa.session_evaluation_receipts (
    session_id text PRIMARY KEY,
    payload_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
