INSERT INTO lisa.policies(
    id, scope, target_agent_name, name, criteria, approved
)
VALUES (%(id)s, %(scope)s, %(target_agent_name)s, %(name)s, %(criteria)s, %(approved)s)
ON CONFLICT (id)
DO UPDATE SET
    scope = EXCLUDED.scope,
    target_agent_name = EXCLUDED.target_agent_name,
    name = EXCLUDED.name,
    criteria = EXCLUDED.criteria,
    approved = EXCLUDED.approved,
    updated_at = now();
