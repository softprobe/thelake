SELECT turn.activity_id, turn.request_key, turn.disposition, turn.is_active,
       activity.kind, activity.status
FROM lisa.activity_chat_turns AS turn
JOIN lisa.activities AS activity ON activity.id = turn.activity_id
WHERE turn.turn_id = %(turn_id)s
ORDER BY turn.is_active DESC, turn.request_key;
