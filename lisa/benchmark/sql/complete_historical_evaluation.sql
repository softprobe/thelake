UPDATE lisa.evaluation_runs
SET status = 'succeeded',
    verdict = %(verdict)s,
    score_id = %(score_id)s,
    started_at = clock_timestamp(),
    finished_at = clock_timestamp()
WHERE id = %(evaluation_id)s;
