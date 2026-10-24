WITH released AS (
    UPDATE lisa.activities
    SET status = 'queued',
        summary = %(summary)s,
        updated_at = clock_timestamp(),
        lease_owner = NULL,
        lease_expires_at = NULL,
        next_event_seq = next_event_seq + 1,
        version = version + 1
    WHERE id = %(activity_id)s
      AND status = 'running'
      AND lease_owner = %(owner)s
      AND fencing_token = %(fencing_token)s
      AND lease_expires_at > clock_timestamp()
    RETURNING id, next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(
    activity_id, seq, event_type, title, summary, actor, reference_type, reference_id
)
SELECT released.id, released.seq, %(event_type)s, %(title)s, %(summary)s, %(actor)s,
       %(reference_type)s, %(reference_id)s
FROM released
RETURNING activity_id;
