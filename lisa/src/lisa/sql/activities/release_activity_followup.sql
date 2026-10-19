UPDATE lisa.activity_followups
SET status = 'approved', claimed_at = NULL
WHERE id = %(id)s AND status = 'claimed';
