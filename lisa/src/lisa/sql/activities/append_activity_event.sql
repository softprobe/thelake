WITH allocated AS (
    UPDATE lisa.activities
    SET next_event_seq = next_event_seq + 1,
        updated_at = now(),
        version = version + 1
    WHERE id = %(activity_id)s
    RETURNING next_event_seq - 1 AS seq
), inserted AS (
INSERT INTO lisa.activity_events(
    activity_id, seq, event_type, title, summary, actor, reference_type, reference_id
)
SELECT %(activity_id)s, allocated.seq, %(event_type)s, %(title)s, %(summary)s,
       %(actor)s, %(reference_type)s, %(reference_id)s
FROM allocated
RETURNING activity_id, seq, event_type, title, summary, actor,
          reference_type, reference_id, created_at
), mirrored AS (
    INSERT INTO lisa.activity_monitor_thread_actions
        (activity_id, seq, event_type, title, summary, actor,
         reference_type, reference_id, status, created_at)
    SELECT i.activity_id, i.seq, i.event_type, i.title, i.summary, i.actor,
           i.reference_type, i.reference_id,
           CASE a.status WHEN 'succeeded' THEN 'success' WHEN 'failed' THEN 'error'
                WHEN 'cancelled' THEN 'stopped' WHEN 'waiting' THEN 'blocked'
                WHEN 'waiting_approval' THEN 'blocked' ELSE 'pending' END, i.created_at
    FROM inserted AS i JOIN lisa.activities AS a ON a.id = i.activity_id
    ON CONFLICT (activity_id, seq) DO UPDATE SET
        event_type = EXCLUDED.event_type, title = EXCLUDED.title,
        summary = EXCLUDED.summary, actor = EXCLUDED.actor,
        reference_type = EXCLUDED.reference_type, reference_id = EXCLUDED.reference_id,
        status = EXCLUDED.status
    RETURNING seq
)
SELECT seq FROM inserted;
