UPDATE lisa.policies
SET approved = true, updated_at = now()
WHERE id = %(id)s AND archived_at IS NULL AND NOT approved
RETURNING id, scope, target_agent_name, name, criteria, approved,
          created_at, updated_at, archived_at;
