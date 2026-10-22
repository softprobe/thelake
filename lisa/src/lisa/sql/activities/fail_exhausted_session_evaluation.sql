WITH eligible AS MATERIALIZED (
    SELECT activity.id
    FROM lisa.activities AS activity
    WHERE activity.id = %(activity_id)s
      AND activity.source = 'session_push'
      AND activity.kind = 'session_evaluation'
      AND activity.status IN ('queued', 'running', 'waiting', 'waiting_approval')
      AND (activity.lease_owner IS NULL OR activity.lease_expires_at <= clock_timestamp())
      AND NOT EXISTS (
          SELECT 1
          FROM lisa.evaluation_runs AS evaluation
          JOIN lisa.score_outbox AS outbox ON outbox.evaluation_id = evaluation.id
          WHERE evaluation.activity_id = activity.id
            AND outbox.status = 'pending'
      )
    FOR UPDATE
), failed_runs AS (
    UPDATE lisa.evaluation_runs AS evaluation
    SET status = 'failed', finished_at = clock_timestamp()
    WHERE evaluation.activity_id IN (SELECT id FROM eligible)
      AND evaluation.status IN ('queued', 'running')
    RETURNING evaluation.activity_id
), finished AS (
    UPDATE lisa.activities AS activity
    SET status = 'failed', summary = %(summary)s,
        next_action = NULL, next_action_at = NULL,
        lease_owner = NULL, lease_expires_at = NULL,
        updated_at = clock_timestamp(), finished_at = clock_timestamp(),
        next_event_seq = next_event_seq + 1, version = version + 1
    FROM eligible
    WHERE activity.id = eligible.id
    RETURNING activity.id, activity.next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, 'failed', 'Session evaluation exhausted retries',
       %(summary)s, 'lisa-session-evaluation-worker'
FROM finished
RETURNING activity_id;
