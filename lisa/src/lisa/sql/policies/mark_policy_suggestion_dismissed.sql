UPDATE lisa.policy_suggestions
SET status = 'dismissed', reviewed_by = %(reviewed_by)s, reviewed_at = now()
WHERE id = %(id)s AND status = 'pending'
RETURNING id;
