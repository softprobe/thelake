SELECT name, criteria, scope, target_agent_name, status, reviewed_at
FROM lisa.policy_suggestions
WHERE status IN ('approved', 'dismissed')
ORDER BY reviewed_at DESC NULLS LAST, id DESC
LIMIT %(limit)s;
