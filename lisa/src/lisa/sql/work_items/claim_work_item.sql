UPDATE lisa.work_items
SET status = 'running',
    lease_owner = %(owner)s,
    lease_expires_at = clock_timestamp() + make_interval(secs => %(lease_seconds)s),
    fencing_token = fencing_token + 1,
    attempt = attempt + 1,
    started_at = COALESCE(started_at, now()),
    updated_at = now(),
    version = version + 1
WHERE id = %(work_item_id)s
  AND (status = 'queued' OR (status = 'running' AND lease_expires_at <= clock_timestamp()))
RETURNING id, fencing_token, lease_expires_at;
