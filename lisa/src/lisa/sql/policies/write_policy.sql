UPDATE lisa.policies
SET name = %(name)s,
    criteria = %(criteria)s,
    approved = CASE
        WHEN name IS DISTINCT FROM %(name)s OR criteria IS DISTINCT FROM %(criteria)s
        THEN false
        ELSE approved
    END,
    updated_at = CASE
        WHEN name IS DISTINCT FROM %(name)s OR criteria IS DISTINCT FROM %(criteria)s
        THEN now()
        ELSE updated_at
    END
WHERE id = %(id)s
  AND scope = %(scope)s
  AND target_agent_name = %(target_agent_name)s
RETURNING id, scope, target_agent_name, name, criteria, approved,
          created_at, updated_at;
