SELECT title, summary
FROM lisa.readable_activity_events
WHERE activity_id = ANY(%s::uuid[]);
