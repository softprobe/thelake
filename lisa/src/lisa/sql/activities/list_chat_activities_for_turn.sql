SELECT activity_id, request_key, disposition, is_active
FROM lisa.activity_chat_turns
WHERE turn_id = %(turn_id)s
ORDER BY is_active DESC, request_key;
