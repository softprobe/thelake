UPDATE lisa.score_outbox
SET status = 'acknowledged', score_id = %(score_id)s,
    acknowledged_at = clock_timestamp(), attempt = attempt + 1
WHERE id = %(outbox_id)s
  AND evaluation_id = %(evaluation_id)s
  AND status = 'pending'
RETURNING id;
