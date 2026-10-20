INSERT INTO lisa.policies(id, scope, target_agent_name, name, criteria, approved)
VALUES (%(id)s, %(scope)s, %(target_agent_name)s, %(name)s, %(criteria)s, false)
RETURNING id, scope, target_agent_name, name, criteria, approved, created_at, updated_at;
