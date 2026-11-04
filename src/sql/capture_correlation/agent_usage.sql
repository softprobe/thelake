SELECT COUNT(*), COALESCE(SUM(byte_size), 0)::BIGINT
FROM {{schema}}.pending_capture_spans
WHERE workspace_id = $1 AND agent_id = $2 AND expires_at > NOW();
