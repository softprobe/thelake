SELECT work_item_id, seq, event_type, title, summary, actor, created_at
FROM lisa.work_events
WHERE work_item_id = %(work_item_id)s
ORDER BY seq;
