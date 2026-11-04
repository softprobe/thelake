UPDATE {{schema}}.pending_capture_spans
SET delivered_at = NOW(), lease_token = NULL, lease_until = NULL
WHERE workspace_id = $1 AND capture_id = $2 AND lease_token = $3;
