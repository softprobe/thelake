INSERT INTO lisa.policy_proposals(
    id, activity_id, scope, target_agent_name, policy_name,
    expected_content, proposed_content, created_by_conversation
)
SELECT
    %(id)s, %(activity_id)s, %(scope)s, %(target_agent_name)s, %(policy_name)s,
    %(expected_content)s, %(proposed_content)s, %(created_by_conversation)s
FROM lisa.activities AS activity
WHERE activity.id = %(activity_id)s
  AND activity.status IN ('queued', 'running', 'waiting', 'waiting_approval')
RETURNING id;
