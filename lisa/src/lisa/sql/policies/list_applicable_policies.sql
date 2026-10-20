SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at, archived_at
FROM (
    SELECT id, scope, target_agent_name, name, criteria, approved,
           created_at, updated_at, archived_at,
           row_number() OVER (
               PARTITION BY name
               ORDER BY CASE WHEN scope = 'agent' THEN 0 ELSE 1 END,
                        approved DESC, updated_at DESC, id DESC
           ) AS scope_priority
    FROM lisa.policies
    WHERE archived_at IS NULL
      AND approved
      AND (scope = 'workspace' OR target_agent_name = %(target_agent_name)s)
) AS effective_policies
WHERE scope_priority = 1
ORDER BY lower(name), id;
