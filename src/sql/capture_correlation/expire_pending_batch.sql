WITH expired AS (
  SELECT ctid
  FROM {{schema}}.pending_capture_spans
  WHERE expires_at <= NOW()
  ORDER BY expires_at
  LIMIT $1
)
DELETE FROM {{schema}}.pending_capture_spans AS pending
USING expired
WHERE pending.ctid = expired.ctid;
