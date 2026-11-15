SELECT span_id, session_id, session_attr_id, http_request_path
FROM softprobe.traces
WHERE workspace_id = ?
  AND trace_id = ?
  AND timestamp >= TIMESTAMP_NS '2020-01-01 00:00:00'
  AND timestamp <= TIMESTAMP_NS '2035-01-01 00:00:00'
ORDER BY span_id ASC;
