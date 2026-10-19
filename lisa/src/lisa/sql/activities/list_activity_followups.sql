SELECT id, activity_id, due_at, trigger_event_type, prompt, status,
       requested_by, requested_at, approved_at, completed_at
FROM lisa.activity_followups
WHERE activity_id = %(activity_id)s
ORDER BY requested_at DESC
LIMIT %(limit)s;
