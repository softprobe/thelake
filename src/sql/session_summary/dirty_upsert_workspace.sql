INSERT INTO {{schema_quoted}}.session_summary_dirty
  (workspace_id, session_id, min_ts_ns, max_ts_ns, updated_at)
SELECT $1::text, input.session_id, input.min_ts_ns, input.max_ts_ns, clock_timestamp()
FROM UNNEST($2::text[], $3::bigint[], $4::bigint[])
  AS input(session_id, min_ts_ns, max_ts_ns)
ON CONFLICT (workspace_id, session_id) DO UPDATE SET
  min_ts_ns = LEAST(session_summary_dirty.min_ts_ns, EXCLUDED.min_ts_ns),
  max_ts_ns = GREATEST(session_summary_dirty.max_ts_ns, EXCLUDED.max_ts_ns),
  updated_at = EXCLUDED.updated_at
