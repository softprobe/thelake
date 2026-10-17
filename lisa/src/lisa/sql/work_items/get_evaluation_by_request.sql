SELECT wi.id, er.policy_id, er.session_id, wi.context_path, wi.status,
       er.verdict, er.score_id
FROM lisa.evaluation_runs AS er
JOIN lisa.work_items AS wi ON wi.id = er.work_item_id
WHERE er.policy_hash = %(policy_hash)s
  AND er.session_id = %(session_id)s
  AND er.request_key = %(request_key)s;
