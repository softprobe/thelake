SELECT activity_id, seq, event_type, title, summary, actor, created_at
FROM lisa.activity_events
WHERE activity_id = %(activity_id)s
ORDER BY seq;
