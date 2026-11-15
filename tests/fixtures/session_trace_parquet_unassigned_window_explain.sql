EXPLAIN ANALYZE
SELECT count(*)
FROM softprobe.{{schema}}.traces
WHERE session_id = ''
  AND timestamp >= TIMESTAMP_NS '2026-09-15 15:00:00'
  AND timestamp < TIMESTAMP_NS '2026-09-15 16:00:00';
