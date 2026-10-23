SELECT activity.id, activity.kind, activity.parent_id, activity.status,
       activity.title, activity.next_action, activity.title_neutralized,
       activity.next_event_seq - 1 AS current_event_seq
FROM lisa.activities AS activity
WHERE activity.id = %(activity_id)s;
