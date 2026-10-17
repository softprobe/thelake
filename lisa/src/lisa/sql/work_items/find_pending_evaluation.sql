SELECT wi.id, wi.context_path, wi.status, er.policy_id,
       er.session_id, er.verdict,
       er.score_id AS evaluation_score_id,
       outbox.id AS outbox_id, outbox.status AS outbox_status,
       outbox.score_payload, outbox.score_id AS outbox_score_id
FROM lisa.evaluation_runs AS er
JOIN lisa.work_items AS wi ON wi.id = er.work_item_id
JOIN lisa.score_outbox AS outbox ON outbox.work_item_id = wi.id
WHERE er.policy_hash = %(policy_hash)s
  AND er.session_id = %(session_id)s
  AND outbox.status = 'pending'
ORDER BY wi.created_at DESC
LIMIT 1;
