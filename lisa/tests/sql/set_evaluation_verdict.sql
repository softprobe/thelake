UPDATE lisa.evaluation_runs
SET verdict = %(verdict)s
WHERE id = %(evaluation_id)s;
