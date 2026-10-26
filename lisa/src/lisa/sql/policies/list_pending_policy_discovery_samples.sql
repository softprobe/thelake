SELECT session_id, agent_name, activity_id
FROM lisa.policy_discovery_samples
WHERE evaluation_complete AND NOT gap_reviewed
ORDER BY sampled_hour, session_id
LIMIT %(limit)s;
