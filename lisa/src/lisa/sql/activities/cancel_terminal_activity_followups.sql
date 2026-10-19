UPDATE lisa.activity_followups AS followup
SET status = 'cancelled'
FROM lisa.activities AS activity
WHERE followup.activity_id = activity.id
  AND activity.status IN ('succeeded', 'failed', 'cancelled')
  AND followup.status IN ('pending', 'approved', 'claimed');
