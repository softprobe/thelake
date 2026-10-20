SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at, archived_at
FROM lisa.policies
WHERE id = %(id)s;
