SELECT policy_id, decision
FROM lisa.evaluation_triage
WHERE session_id = %(session_id)s
  AND policy_id = ANY(%(policy_ids)s);
