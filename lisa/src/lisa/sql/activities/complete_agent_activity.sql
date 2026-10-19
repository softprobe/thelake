WITH finished AS (
    UPDATE lisa.activities AS activity
    SET status = %(status)s, summary = %(summary)s,
        updated_at = clock_timestamp(), finished_at = clock_timestamp(),
        next_event_seq = next_event_seq + 1, version = version + 1
    WHERE activity.id = %(activity_id)s
      AND activity.status = 'queued'
      AND activity.kind <> 'policy_evaluation'
      AND NOT EXISTS (
          SELECT 1 FROM lisa.evaluation_runs AS evaluation
          WHERE evaluation.activity_id = activity.id
      )
    RETURNING activity.id, activity.next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, %(status)s, %(title)s,
       %(summary)s, 'lisa'
FROM finished
RETURNING activity_id;
