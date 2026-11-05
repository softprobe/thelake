SELECT COUNT(*)
FROM {{schema}}.capture_session_mappings
WHERE workspace_id = $1 AND agent_id = $2 AND expires_at > NOW();
