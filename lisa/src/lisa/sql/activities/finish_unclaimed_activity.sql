WITH finished AS (
    UPDATE lisa.activities
    SET status = %(status)s, summary = %(summary)s,
        updated_at = clock_timestamp(), finished_at = clock_timestamp(),
        lease_owner = NULL, lease_expires_at = NULL,
        next_event_seq = next_event_seq + 1, version = version + 1
    WHERE id = %(activity_id)s
      AND status IN ('queued', 'running', 'waiting', 'waiting_approval')
      AND (lease_owner IS NULL OR lease_expires_at <= clock_timestamp())
    RETURNING id, next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, %(status)s, %(title)s,
       %(summary)s, 'lisa'
FROM finished
RETURNING activity_id;
