UPDATE lisa.evaluation_runs
SET verdict = %(verdict)s,
    score_id = %(score_id)s,
    finished_at = clock_timestamp()
WHERE work_item_id = %(work_item_id)s;
