SELECT COUNT(*) FROM dst_lake.{{table}}
WHERE timestamp >= '{{day_start}}'::TIMESTAMPTZ::TIMESTAMP_NS
  AND timestamp < ('{{day_start}}'::TIMESTAMPTZ + INTERVAL '1 day')::TIMESTAMP_NS
