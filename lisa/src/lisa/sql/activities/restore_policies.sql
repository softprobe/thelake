INSERT INTO lisa.policies(
    id, scope, target_agent_name, name, criteria, approved,
    created_at, updated_at, archived_at
)
VALUES (
    %(id)s, %(scope)s, %(target_agent_name)s, %(name)s, %(criteria)s, %(approved)s,
    %(created_at)s, %(updated_at)s, %(archived_at)s
)
ON CONFLICT (id) DO UPDATE SET
    scope = EXCLUDED.scope,
    target_agent_name = EXCLUDED.target_agent_name,
    name = EXCLUDED.name,
    criteria = EXCLUDED.criteria,
    approved = EXCLUDED.approved,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    archived_at = EXCLUDED.archived_at;
