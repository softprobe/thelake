SELECT id, kind, parent_id, status, title, title_neutralized, next_action,
       next_event_seq - 1 AS current_event_seq
FROM lisa.activities
WHERE id = %(activity_id)s
FOR UPDATE;
