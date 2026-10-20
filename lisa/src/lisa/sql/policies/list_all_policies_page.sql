SELECT id, scope, target_agent_name, name, criteria, approved,
       created_at, updated_at
FROM lisa.policies
ORDER BY
  SPLIT_PART(id, '.', 2)::BIGINT,
  COALESCE(NULLIF(SPLIT_PART(id, '.', 3), ''), '0')::BIGINT,
  lower(name), id
LIMIT %(limit)s OFFSET %(offset)s;
