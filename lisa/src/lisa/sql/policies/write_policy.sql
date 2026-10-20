INSERT INTO lisa.policies(id, scope, target_agent_name, name, criteria, approved)
VALUES (%(id)s, %(scope)s, %(target_agent_name)s, %(name)s, %(criteria)s, false)
ON CONFLICT (id) DO UPDATE SET
    criteria = EXCLUDED.criteria,
    updated_at = now()
WHERE lisa.policies.approved = false
  AND lisa.policies.archived_at IS NULL
RETURNING id, scope, target_agent_name, name, criteria, approved,
          created_at, updated_at, archived_at;
