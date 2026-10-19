UPDATE lisa.activities SET title = %(title)s, updated_at = now(), version = version + 1
WHERE id = %(activity_id)s;
