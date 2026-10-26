INSERT INTO lisa.policy_discovery_samples
    (session_id, agent_name, activity_id, sampled_hour)
SELECT %(session_id)s, %(agent_name)s, %(activity_id)s, %(sampled_hour)s
WHERE (
    SELECT count(*) FROM lisa.policy_discovery_samples
    WHERE sampled_hour = %(sampled_hour)s
) < %(hourly_limit)s
ON CONFLICT (session_id) DO NOTHING
RETURNING session_id;
