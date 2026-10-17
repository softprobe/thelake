WITH released AS (
    UPDATE lisa.work_items
    SET status = 'queued',
        summary = %(summary)s,
        updated_at = clock_timestamp(),
        lease_owner = NULL,
        lease_expires_at = NULL,
        next_event_seq = next_event_seq + 1,
        version = version + 1
    WHERE id = %(work_item_id)s
      AND status = 'running'
      AND lease_owner = %(owner)s
      AND fencing_token = %(fencing_token)s
      AND lease_expires_at > clock_timestamp()
    RETURNING id, next_event_seq - 1 AS seq
)
INSERT INTO lisa.work_events(work_item_id, seq, event_type, title, summary, actor)
SELECT released.id, released.seq, %(event_type)s, %(title)s, %(summary)s, %(actor)s
FROM released
RETURNING work_item_id;
