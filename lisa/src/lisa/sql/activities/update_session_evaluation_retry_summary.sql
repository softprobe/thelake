UPDATE lisa.activities
SET summary = %(summary)s,
    updated_at = clock_timestamp(),
    version = version + 1
WHERE id = %(activity_id)s;
