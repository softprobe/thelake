SELECT DISTINCT strftime(timestamp, '%Y-%m-%d') AS day
FROM {{src_db}}.{{table}}
WHERE timestamp >= '{{since_ts}}'::TIMESTAMPTZ::TIMESTAMP_NS
  AND timestamp < '{{until_ts}}'::TIMESTAMPTZ::TIMESTAMP_NS
ORDER BY day;
