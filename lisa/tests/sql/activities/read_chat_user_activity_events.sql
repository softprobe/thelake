SELECT event_type, title, summary
FROM lisa.readable_activity_events
WHERE activity_id = %s::uuid
  AND event_type = 'chat.user';
