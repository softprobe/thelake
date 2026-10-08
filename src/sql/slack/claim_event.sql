INSERT INTO __TABLE__ AS current_event
  (team_id, event_id, claim_id, state, lease_until)
VALUES ($1, $2, $3, 'processing', now() + ($4::bigint * INTERVAL '1 second'))
ON CONFLICT (team_id, event_id) DO UPDATE SET
  claim_id = EXCLUDED.claim_id,
  state = 'processing',
  lease_until = EXCLUDED.lease_until,
  completed_at = NULL
WHERE current_event.state = 'processing'
  AND current_event.lease_until <= now()
RETURNING state;
