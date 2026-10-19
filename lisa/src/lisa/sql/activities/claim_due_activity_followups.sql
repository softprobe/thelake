WITH due AS (
    SELECT followup.id FROM lisa.activities AS activity
    JOIN lisa.activity_followups AS followup ON followup.activity_id = activity.id
    WHERE activity.status IN ('queued', 'running', 'waiting', 'waiting_approval')
      AND (followup.status = 'approved' OR
           (followup.status = 'claimed' AND followup.claimed_at < now() - interval '10 minutes'))
      AND (
        (due_at IS NOT NULL AND due_at <= now()) OR
        (trigger_event_type IS NOT NULL AND EXISTS (
            SELECT 1 FROM lisa.activity_events AS event
            WHERE event.activity_id = followup.activity_id
              AND event.event_type = followup.trigger_event_type
        ))
    )
    ORDER BY followup.due_at
    LIMIT %(limit)s
    FOR UPDATE OF activity SKIP LOCKED
)
UPDATE lisa.activity_followups AS followup
SET status = 'claimed', claimed_at = now()
FROM due
WHERE followup.id = due.id
RETURNING followup.id, followup.activity_id, followup.prompt;
