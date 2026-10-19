UPDATE lisa.activities
SET status = %(status)s,
    next_action = %(next_action)s,
    next_action_at = %(next_action_at)s,
    started_at = CASE WHEN %(status)s = 'running' THEN COALESCE(started_at, now()) ELSE started_at END,
    updated_at = now(),
    version = version + 1
WHERE id = %(activity_id)s
  AND status IN ('queued', 'running', 'waiting', 'waiting_approval')
  AND kind <> 'policy_evaluation'
  AND NOT EXISTS (
      SELECT 1 FROM lisa.evaluation_runs AS evaluation
      WHERE evaluation.activity_id = lisa.activities.id
  )
  AND (lease_owner IS NULL OR lease_expires_at <= now())
RETURNING title;
