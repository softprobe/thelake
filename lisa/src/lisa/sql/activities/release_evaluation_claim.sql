UPDATE lisa.activities
SET status = 'queued',
    updated_at = clock_timestamp(),
    lease_owner = NULL,
    lease_expires_at = NULL,
    version = version + 1
WHERE id = %(activity_id)s
  AND status = 'running'
  AND lease_owner = %(owner)s
  AND fencing_token = %(fencing_token)s
  AND lease_expires_at > clock_timestamp()
RETURNING id;
