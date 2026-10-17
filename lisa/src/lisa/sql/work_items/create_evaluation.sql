INSERT INTO lisa.evaluation_runs(
    work_item_id, policy_id, target_agent_name, session_id, policy_name, policy_hash,
    judge_config, request_key
)
VALUES (
    %(work_item_id)s, %(policy_id)s, %(target_agent_name)s, %(session_id)s, %(policy_name)s,
    %(policy_hash)s, %(judge_config)s::jsonb, %(request_key)s
);
