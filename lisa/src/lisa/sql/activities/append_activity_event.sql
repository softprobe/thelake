WITH allocated AS (
    UPDATE lisa.activities
    SET next_event_seq = next_event_seq + 1,
        updated_at = now(),
        version = version + 1
    WHERE id = %(activity_id)s
      AND status IN ('queued', 'running')
    RETURNING next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT %(activity_id)s, allocated.seq, %(event_type)s, %(title)s, %(summary)s, %(actor)s
FROM allocated
RETURNING seq;
