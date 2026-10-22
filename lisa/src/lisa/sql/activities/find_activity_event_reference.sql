SELECT seq
FROM lisa.activity_events
WHERE activity_id = %(activity_id)s
  AND reference_type = %(reference_type)s
  AND reference_id = %(reference_id)s;
