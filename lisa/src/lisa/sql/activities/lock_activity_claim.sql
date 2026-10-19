SELECT id
FROM lisa.activities
WHERE id = %(activity_id)s
  AND status = 'running'
  AND lease_owner = %(owner)s
  AND fencing_token = %(fencing_token)s
  AND lease_expires_at > clock_timestamp()
FOR UPDATE;
