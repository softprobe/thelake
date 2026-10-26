SELECT id, name, criteria, scope, target_agent_name, rationale, signal_ids,
       evidence, overlap_details, status, policy_id, created_at, reviewed_at
FROM lisa.policy_suggestions
WHERE (CAST(%(status)s AS text) IS NULL OR status = %(status)s)
ORDER BY created_at DESC, id
LIMIT %(limit)s;
