SELECT session_id, start_time_ns, end_time_ns
FROM {{schema_quoted}}.session_summary
WHERE workspace_id = $1 AND session_id = ANY($2::text[])
