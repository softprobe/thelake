SELECT span_json, byte_size
FROM {{schema}}.pending_capture_spans
WHERE workspace_id = $1 AND capture_id = $2 AND trace_id = $3 AND span_id = $4;
