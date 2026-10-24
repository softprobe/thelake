SELECT id AS evaluation_id, policy_name AS evaluation_name, verdict, status
FROM lisa.evaluation_runs
WHERE activity_id = %(activity_id)s
ORDER BY started_at ASC NULLS LAST, id ASC;
