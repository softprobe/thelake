SELECT trace_id, CAST(attributes['k'] AS VARCHAR)
FROM dst_lake.thelake_v2.traces
ORDER BY trace_id;
