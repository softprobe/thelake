SELECT EXISTS (
    SELECT 1
    FROM lisa.evaluation_runs
    WHERE policy_id = %(policy_id)s
) AS has_history;
