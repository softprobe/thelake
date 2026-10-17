WITH allocated AS (
    UPDATE lisa.work_items
    SET next_event_seq = next_event_seq + 1,
        updated_at = now(),
        version = version + 1
    WHERE id = %(work_item_id)s
      AND status = 'queued'
    RETURNING next_event_seq - 1 AS seq
)
INSERT INTO lisa.work_events(work_item_id, seq, event_type, title, summary, actor)
SELECT %(work_item_id)s, allocated.seq, %(event_type)s, %(title)s, %(summary)s, %(actor)s
FROM allocated
RETURNING seq;
