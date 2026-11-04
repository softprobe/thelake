INSERT INTO {{schema}}.pending_capture_spans
  (workspace_id, capture_id, trace_id, span_id, session_id, span_json, byte_size, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW() + INTERVAL '10 minutes')
ON CONFLICT (workspace_id, capture_id, trace_id, span_id) DO NOTHING;
