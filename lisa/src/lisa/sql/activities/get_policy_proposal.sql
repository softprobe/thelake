SELECT id, activity_id, scope, target_agent_name, policy_name,
       expected_content, proposed_content, status, created_by_conversation
FROM lisa.policy_proposals
WHERE id = %(id)s AND status = 'pending';
