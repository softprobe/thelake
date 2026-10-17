UPDATE lisa.policies
SET archived_at = now(), updated_at = now()
WHERE scope = %(scope)s
  AND target_agent_name = %(target_agent_name)s
  AND archived_at IS NULL
  AND NOT (name = ANY(%(names)s));
