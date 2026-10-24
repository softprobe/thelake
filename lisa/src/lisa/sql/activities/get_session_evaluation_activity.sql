SELECT id, source, kind, status, next_event_seq - 1 AS event_watermark
FROM lisa.activities
WHERE id = %(activity_id)s
FOR UPDATE;
