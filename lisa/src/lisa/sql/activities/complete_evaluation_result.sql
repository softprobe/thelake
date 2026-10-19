UPDATE lisa.evaluation_runs
SET verdict = %(verdict)s, status = 'succeeded',
    score_id = %(score_id)s, finished_at = clock_timestamp()
WHERE id = %(evaluation_id)s;
