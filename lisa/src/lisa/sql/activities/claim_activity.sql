UPDATE lisa.activities
SET status = 'running',
    lease_owner = %(owner)s,
    lease_expires_at = clock_timestamp() + make_interval(secs => %(lease_seconds)s),
    fencing_token = CASE
        WHEN status = 'running' AND lease_owner = %(owner)s
             AND lease_expires_at > clock_timestamp() THEN fencing_token
        ELSE fencing_token + 1 END,
    attempt = CASE
        WHEN status = 'running' AND lease_owner = %(owner)s
             AND lease_expires_at > clock_timestamp() THEN attempt
        ELSE attempt + 1 END,
    started_at = COALESCE(started_at, now()),
    updated_at = now(),
    version = version + 1
WHERE id = %(activity_id)s
  AND (status = 'queued'
       OR (status = 'running' AND (lease_expires_at <= clock_timestamp()
                                   OR lease_owner = %(owner)s)))
RETURNING id, fencing_token, lease_expires_at;
