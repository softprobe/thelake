INSERT INTO {{schema}}.capture_session_mappings
  (workspace_id, agent_id, trace_id, session_id, expires_at, updated_at)
VALUES ($1, $2, $3, $4, NOW() + INTERVAL '10 minutes', NOW())
ON CONFLICT (workspace_id, agent_id, trace_id)
DO UPDATE SET expires_at = EXCLUDED.expires_at, updated_at = NOW()
WHERE {{schema}}.capture_session_mappings.session_id = EXCLUDED.session_id
RETURNING session_id;
