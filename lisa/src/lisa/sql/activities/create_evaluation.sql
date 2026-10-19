INSERT INTO lisa.evaluation_runs(
    id, activity_id, policy_id, target_agent_name, session_id, policy_name, policy_hash,
    judge_config, request_key
)
VALUES (
    %(id)s, %(activity_id)s, %(policy_id)s, %(target_agent_name)s, %(session_id)s, %(policy_name)s,
    %(policy_hash)s, %(judge_config)s::jsonb, %(request_key)s
);
