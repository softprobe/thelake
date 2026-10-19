SELECT id, activity_id, scope, target_agent_name, policy_name,
       expected_content, proposed_content, status, created_by_conversation,
       created_at
FROM lisa.policy_proposals
WHERE status = 'pending'
ORDER BY created_at DESC
LIMIT %(limit)s;
