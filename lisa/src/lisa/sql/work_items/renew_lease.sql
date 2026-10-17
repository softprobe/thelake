UPDATE lisa.work_items
SET lease_expires_at = clock_timestamp() + make_interval(secs => %(lease_seconds)s),
    updated_at = clock_timestamp(),
    version = version + 1
WHERE id = %(work_item_id)s
  AND status = 'running'
  AND lease_owner = %(owner)s
  AND fencing_token = %(fencing_token)s
  AND lease_expires_at > clock_timestamp()
RETURNING id, fencing_token, lease_expires_at;
