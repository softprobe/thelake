WITH finished AS (
    UPDATE lisa.activities AS activity
    SET status = 'succeeded', finished_at = clock_timestamp(),
        updated_at = clock_timestamp(), lease_owner = NULL, lease_expires_at = NULL,
        next_event_seq = next_event_seq + 1, version = version + 1
    WHERE activity.id = %(activity_id)s
      AND activity.status IN ('queued', 'running')
      AND (
          (activity.status = 'queued' AND activity.lease_owner IS NULL
           AND %(actor)s::text IS NULL AND %(fencing_token)s::bigint IS NULL)
          OR
          (activity.status = 'running' AND activity.lease_owner = %(actor)s
           AND activity.fencing_token = %(fencing_token)s
           AND activity.lease_expires_at > clock_timestamp())
      )
      AND NOT EXISTS (
          SELECT 1
          FROM lisa.evaluation_runs AS runs
          WHERE runs.activity_id = activity.id
            AND (runs.verdict IS NULL OR EXISTS (
                SELECT 1 FROM lisa.score_outbox AS outbox
                WHERE outbox.evaluation_id = runs.id AND outbox.status = 'pending'
            ))
      )
    RETURNING activity.id, activity.next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, 'succeeded', 'Activity completed',
       'All evaluations for this activity are complete.', 'lisa'
FROM finished
RETURNING activity_id;
