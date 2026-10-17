SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at, archived_at
FROM lisa.policies
WHERE scope = %(scope)s
  AND COALESCE(target_agent_name, '') = %(target_agent_name)s
ORDER BY name, id;
