UPDATE lisa.policy_suggestions
SET status = 'approved', policy_id = %(policy_id)s, reviewed_by = %(reviewed_by)s,
    reviewed_at = now(), name = %(name)s, criteria = %(criteria)s
WHERE id = %(id)s AND status = 'pending'
RETURNING id;
