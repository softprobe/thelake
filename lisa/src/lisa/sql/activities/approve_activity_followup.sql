UPDATE lisa.activity_followups
SET status = 'approved', approved_at = now()
WHERE id = %(id)s AND status = 'pending'
RETURNING id, activity_id;
