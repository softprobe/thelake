UPDATE lisa.activities
SET summary = 'Evaluation completed after a retry.',
    updated_at = clock_timestamp(),
    version = version + 1
WHERE id = %(activity_id)s
  AND source = 'session_push'
  AND kind = 'session_evaluation'
  AND status = 'succeeded'
  AND summary LIKE 'Evaluation attempt %% failed:%%'
RETURNING id;
