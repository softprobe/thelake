UPDATE lisa.activity_followups
SET status = 'completed', completed_at = now()
WHERE id = %(id)s AND status = 'claimed';
