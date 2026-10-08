UPDATE __TABLE__
SET state = 'complete', lease_until = NULL, completed_at = now()
WHERE team_id = $1 AND event_id = $2 AND claim_id = $3 AND state = 'processing';
