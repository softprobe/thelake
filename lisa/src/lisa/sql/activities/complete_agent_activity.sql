WITH finished AS (
    UPDATE lisa.activities AS activity
    SET status = %(status)s, summary = %(summary)s,
        updated_at = clock_timestamp(), finished_at = clock_timestamp(),
        next_event_seq = next_event_seq + 1, version = version + 1
    WHERE activity.id = %(activity_id)s
      AND activity.status IN ('queued', 'running', 'waiting', 'waiting_approval')
      AND activity.kind <> 'policy_evaluation'
      AND NOT EXISTS (
          SELECT 1 FROM lisa.evaluation_runs AS evaluation
          WHERE evaluation.activity_id = activity.id
      )
      AND (activity.lease_owner IS NULL OR activity.lease_expires_at <= now())
      AND NOT EXISTS (
          SELECT 1 FROM lisa.activity_followups AS followup
          WHERE followup.activity_id = activity.id
            AND followup.status IN ('pending', 'approved', 'claimed')
      )
      AND NOT EXISTS (
          SELECT 1 FROM lisa.policy_proposals AS proposal
          WHERE proposal.activity_id = activity.id
            AND proposal.status IN ('pending', 'applying')
      )
    RETURNING activity.id, activity.next_event_seq - 1 AS seq
)
INSERT INTO lisa.activity_events(activity_id, seq, event_type, title, summary, actor)
SELECT finished.id, finished.seq, %(status)s, %(title)s,
       %(summary)s, 'lisa'
FROM finished
RETURNING activity_id;
