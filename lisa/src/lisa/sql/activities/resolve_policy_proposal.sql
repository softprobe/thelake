UPDATE lisa.policy_proposals
SET status = %(status)s, resolved_at = now(), resolved_by_conversation = %(conversation_id)s,
    applied_at = CASE WHEN %(status)s = 'applied' THEN now() ELSE applied_at END
WHERE id = %(id)s
  AND ((%(status)s = 'rejected' AND status = 'pending')
       OR (%(status)s IN ('applied', 'stale') AND status = 'applying'))
RETURNING id, activity_id;
