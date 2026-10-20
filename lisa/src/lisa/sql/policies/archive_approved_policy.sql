UPDATE lisa.policies
SET archived_at = now(), updated_at = now()
WHERE scope = %(scope)s
  AND target_agent_name = %(target_agent_name)s
  AND name = %(name)s
  AND id <> %(id)s
  AND approved
  AND archived_at IS NULL;
