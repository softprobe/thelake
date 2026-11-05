INSERT INTO {{schema_quoted}}.session_summary_dirty
  (session_id, min_ts_ns, max_ts_ns, updated_at)
SELECT input.session_id, input.min_ts_ns, input.max_ts_ns, clock_timestamp()
FROM UNNEST($1::text[], $2::bigint[], $3::bigint[])
  AS input(session_id, min_ts_ns, max_ts_ns)
ON CONFLICT (session_id) DO UPDATE SET
  min_ts_ns = LEAST(session_summary_dirty.min_ts_ns, EXCLUDED.min_ts_ns),
  max_ts_ns = GREATEST(session_summary_dirty.max_ts_ns, EXCLUDED.max_ts_ns),
  updated_at = EXCLUDED.updated_at
