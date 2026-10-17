SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at, archived_at
FROM lisa.policies
WHERE archived_at IS NULL
ORDER BY scope, target_agent_name, lower(name), id;
