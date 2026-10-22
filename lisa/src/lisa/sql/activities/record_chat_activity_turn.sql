INSERT INTO lisa.activity_chat_turns(turn_id, request_key, activity_id, disposition)
VALUES (%(turn_id)s, %(request_key)s, %(activity_id)s, %(disposition)s)
ON CONFLICT (turn_id, request_key) DO NOTHING
RETURNING activity_id;
