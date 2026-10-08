DELETE FROM __TABLE__
WHERE team_id = $1 AND event_id = $2 AND claim_id = $3 AND state = 'processing';
