WITH candidates AS (
  SELECT workspace_id, agent_id, trace_id, span_id
  FROM {{schema}}.pending_capture_spans
  WHERE workspace_id = $1 AND agent_id = $2 AND session_id IS NOT NULL
    AND delivered_at IS NULL AND expires_at > NOW()
    AND (lease_until IS NULL OR lease_until <= NOW())
  ORDER BY expires_at, trace_id, span_id
  FOR UPDATE SKIP LOCKED
  LIMIT $4
)
UPDATE {{schema}}.pending_capture_spans AS pending
SET lease_token = $3, lease_until = NOW() + INTERVAL '30 seconds'
FROM candidates
WHERE pending.workspace_id = candidates.workspace_id
  AND pending.agent_id = candidates.agent_id
  AND pending.trace_id = candidates.trace_id
  AND pending.span_id = candidates.span_id
RETURNING pending.span_json, pending.session_id, pending.trace_id, pending.span_id;
