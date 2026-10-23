WITH renamed AS (
    UPDATE lisa.activities
    SET title = CASE WHEN title_neutralized THEN title ELSE %(title)s END,
        updated_at = now(), version = version + 1
    WHERE id = %(activity_id)s
    RETURNING id
)
UPDATE lisa.activity_feed_entries AS feed
SET search_title = %(title)s, updated_at = now()
FROM renamed
WHERE feed.activity_id = renamed.id;
