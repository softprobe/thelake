WITH finished AS (
    UPDATE lisa.activities AS activity
    SET status = %(status)s,
        summary = COALESCE(%(summary)s, summary),
        finished_at = now(),
        updated_at = now(),
        lease_owner = NULL,
        lease_expires_at = NULL,
        next_event_seq = next_event_seq + 1,
        version = version + 1
    WHERE activity.id = %(activity_id)s
      AND activity.status = 'running'
      AND activity.lease_owner = %(owner)s
      AND activity.fencing_token = %(fencing_token)s
      AND activity.lease_expires_at > clock_timestamp()
      AND (
          %(status)s <> 'succeeded'
          OR EXISTS (
              SELECT 1
              FROM lisa.score_outbox AS outbox
              JOIN lisa.evaluation_runs AS runs ON runs.id = outbox.evaluation_id
              WHERE runs.activity_id = activity.id AND outbox.status = 'acknowledged'
          )
      )
    RETURNING activity.id, activity.next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, %(event_type)s, %(title)s, %(summary)s, %(actor)s
FROM finished
RETURNING activity_id;
