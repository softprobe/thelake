SELECT session_id, start_time_ns, end_time_ns
FROM {{schema_quoted}}.session_summary
WHERE session_id = ANY($1::text[])
