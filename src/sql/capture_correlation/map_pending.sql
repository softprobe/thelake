UPDATE {{schema}}.pending_capture_spans
SET session_id = $4
WHERE workspace_id = $1 AND capture_id = $2 AND trace_id = $3
  AND delivered_at IS NULL AND expires_at > NOW()
  AND (session_id IS NULL OR session_id = $4);
