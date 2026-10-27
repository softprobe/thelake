SELECT session_id, agent_name, activity_id
FROM lisa.policy_discovery_samples
WHERE session_id = %(session_id)s;
