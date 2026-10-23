WITH finished AS (
    UPDATE lisa.activities AS activity
    SET status = %(status)s, summary = %(summary)s,
        next_action = NULL, next_action_at = NULL,
        updated_at = clock_timestamp(), finished_at = clock_timestamp(),
        next_event_seq = next_event_seq + 1, version = version + 1
    WHERE activity.id = %(activity_id)s
      AND activity.status IN ('queued', 'running', 'waiting', 'waiting_approval')
      AND activity.kind <> 'policy_evaluation'
      AND (
          NOT EXISTS (
              SELECT 1 FROM lisa.evaluation_runs AS evaluation
              WHERE evaluation.activity_id = activity.id
          )
          OR (
              activity.kind = 'chat_request'
              AND NOT EXISTS (
                  SELECT 1 FROM lisa.evaluation_runs AS evaluation
                  WHERE evaluation.activity_id = activity.id
                    AND (
                        evaluation.status <> 'succeeded'
                        OR evaluation.verdict IS NULL
                        OR EXISTS (
                            SELECT 1 FROM lisa.score_outbox AS outbox
                            WHERE outbox.evaluation_id = evaluation.id
                              AND outbox.status = 'pending'
                        )
                    )
              )
          )
      )
      AND (activity.lease_owner IS NULL OR activity.lease_expires_at <= now())
    RETURNING activity.id, activity.next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(
    activity_id, seq, chat_turn_id, event_type, title, summary, actor
)
SELECT finished.id, finished.seq, %(chat_turn_id)s, %(status)s, %(title)s,
       %(summary)s, 'lisa'
FROM finished
RETURNING activity_id;
