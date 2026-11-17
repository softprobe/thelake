SELECT trace_id, CAST(attributes['k'] AS VARCHAR)
FROM dst_lake.thelake_v2.traces
WHERE timestamp >= '2026-11-16T00:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS
  AND timestamp < '2026-11-17T00:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS
ORDER BY trace_id;
