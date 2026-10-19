SELECT er.id AS evaluation_id, er.activity_id, a.context_path, a.status, er.policy_id,
       er.session_id, er.verdict,
       er.score_id AS evaluation_score_id,
       outbox.id AS outbox_id, outbox.status AS outbox_status,
       outbox.score_payload, outbox.score_id AS outbox_score_id
FROM lisa.evaluation_runs AS er
JOIN lisa.activities AS a ON a.id = er.activity_id
JOIN lisa.score_outbox AS outbox ON outbox.evaluation_id = er.id
WHERE er.target_agent_name = %(target_agent_name)s
  AND er.session_id = %(session_id)s
  AND outbox.status = 'pending'
ORDER BY a.created_at ASC;
