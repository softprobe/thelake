INSERT INTO lisa.policies(
    id, scope, target_agent_name, name, criteria, approved, archived_at
)
VALUES (%(id)s, %(scope)s, %(target_agent_name)s, %(name)s, %(criteria)s, %(approved)s, NULL)
ON CONFLICT (id)
DO UPDATE SET
    scope = EXCLUDED.scope,
    target_agent_name = EXCLUDED.target_agent_name,
    name = EXCLUDED.name,
    criteria = EXCLUDED.criteria,
    approved = EXCLUDED.approved,
    archived_at = NULL,
    updated_at = now();
