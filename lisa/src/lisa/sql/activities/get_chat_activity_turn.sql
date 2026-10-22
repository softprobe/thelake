SELECT activity_id, disposition
FROM lisa.activity_chat_turns
WHERE turn_id = %(turn_id)s AND request_key = %(request_key)s;
