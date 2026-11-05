WITH expired AS (
  SELECT ctid
  FROM {{schema}}.capture_session_mappings
  WHERE expires_at <= NOW()
  ORDER BY expires_at
  LIMIT $1
)
DELETE FROM {{schema}}.capture_session_mappings AS mappings
USING expired
WHERE mappings.ctid = expired.ctid;
