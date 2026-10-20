DELETE FROM lisa.policies
WHERE id = %(id)s
  AND scope = %(scope)s
  AND target_agent_name = %(target_agent_name)s
RETURNING id;
