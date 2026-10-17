UPDATE lisa.score_outbox
SET status = 'acknowledged',
    score_id = %(score_id)s,
    acknowledged_at = clock_timestamp()
WHERE id = %(outbox_id)s
  AND work_item_id = %(work_item_id)s
  AND idempotency_key = %(score_id)s
  AND score_payload->>'string_value' = %(verdict)s
  AND status = 'pending'
RETURNING id;
