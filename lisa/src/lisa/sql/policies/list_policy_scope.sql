SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at
FROM lisa.policies
WHERE scope = %(scope)s
  AND target_agent_name = %(target_agent_name)s
ORDER BY lower(name), id;
