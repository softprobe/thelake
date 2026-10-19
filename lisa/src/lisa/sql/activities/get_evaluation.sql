SELECT er.id AS evaluation_id, er.activity_id, er.policy_id, er.session_id,
       a.context_path, a.status, er.verdict, er.score_id
FROM lisa.evaluation_runs AS er
JOIN lisa.activities AS a ON a.id = er.activity_id
WHERE er.id = %(evaluation_id)s;
