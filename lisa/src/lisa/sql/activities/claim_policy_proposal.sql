UPDATE lisa.policy_proposals
SET status = 'applying', applying_at = now()
WHERE id = %(id)s
  AND (status = 'pending' OR (status = 'applying' AND applying_at < now() - interval '10 minutes'))
RETURNING id, activity_id, scope, target_agent_name, policy_name,
          expected_content, proposed_content, created_by_conversation;
