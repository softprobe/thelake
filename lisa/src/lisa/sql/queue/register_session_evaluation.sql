INSERT INTO lisa.session_evaluation_receipts (session_id, payload_hash)
VALUES (%(session_id)s, %(payload_hash)s)
ON CONFLICT (session_id) DO NOTHING
RETURNING session_id;
