INSERT INTO lisa.activity_followups(
    id, activity_id, due_at, trigger_event_type, prompt, requested_by
)
SELECT %(id)s, a.id, %(due_at)s, %(trigger_event_type)s, %(prompt)s, %(requested_by)s
FROM lisa.activities AS a
WHERE a.id = %(activity_id)s AND a.status IN ('queued', 'running', 'waiting')
RETURNING id;
