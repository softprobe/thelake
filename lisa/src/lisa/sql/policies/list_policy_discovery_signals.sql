SELECT id, session_id, activity_id, agent_name, summary, span_ids, evidence, created_at
FROM lisa.policy_discovery_signals
WHERE state = 'pending'
ORDER BY created_at, id
LIMIT %(limit)s;
