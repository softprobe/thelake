SELECT er.id AS evaluation_id, er.activity_id, er.policy_id, er.session_id,
       a.context_path, er.status, er.verdict, er.score_id
FROM lisa.evaluation_runs AS er
JOIN lisa.activities AS a ON a.id = er.activity_id
WHERE er.policy_hash = %(policy_hash)s
  AND er.session_id = %(session_id)s
  AND er.request_key = %(request_key)s;
