SELECT seq, event_type, title, summary, actor, reference_id
FROM lisa.activity_events
WHERE activity_id = %(activity_id)s
  AND reference_type = 'session_evaluation_attempt'
  AND (
      reference_id = %(delivery_key)s
      OR left(reference_id, length(%(delivery_key)s || ':')) = %(delivery_key)s || ':'
  )
ORDER BY seq DESC
LIMIT 1;
