SELECT id, candidate_hash, name, criteria, scope, target_agent_name, rationale,
       signal_ids, evidence, overlap_details, status, policy_id, created_at, reviewed_at
FROM lisa.policy_suggestions
WHERE id = %(id)s
FOR UPDATE;
