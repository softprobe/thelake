SELECT start_time_ns, COALESCE(end_time_ns, start_time_ns) AS end_time_ns
FROM {{schema_quoted}}.session_summary
WHERE {{ownership}}session_id = $1 LIMIT 1
