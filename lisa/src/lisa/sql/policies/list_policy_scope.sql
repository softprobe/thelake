SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at, archived_at
FROM lisa.policies
WHERE scope = %(scope)s
  AND target_agent_name = %(target_agent_name)s
  AND archived_at IS NULL
ORDER BY lower(name), id;
