SELECT EXISTS (SELECT 1 FROM lisa.activities WHERE id = %(activity_id)s) AS found;
