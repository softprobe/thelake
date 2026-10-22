UPDATE lisa.activity_chat_turns
SET is_active = (request_key = %(request_key)s)
WHERE turn_id = %(turn_id)s;
