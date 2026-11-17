SELECT COUNT(*)
FROM {{db}}.{{table}}
WHERE timestamp >= '{{start_ts}}'::TIMESTAMPTZ::TIMESTAMP_NS
  AND timestamp < '{{end_ts}}'::TIMESTAMPTZ::TIMESTAMP_NS;
