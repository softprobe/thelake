SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at, archived_at
FROM lisa.policies
WHERE archived_at IS NULL
ORDER BY
  CASE WHEN id ~ '^P1[.][1-9][0-9]*$' THEN 0 ELSE 1 END,
  CASE WHEN id ~ '^P1[.][1-9][0-9]*$' THEN SUBSTRING(id FROM 4)::BIGINT END,
  id
LIMIT %(limit)s OFFSET %(offset)s;
