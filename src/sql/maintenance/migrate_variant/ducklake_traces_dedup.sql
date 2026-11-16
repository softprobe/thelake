AND NOT EXISTS (
    SELECT 1 FROM dst_lake.traces d
    WHERE d.trace_id = s.trace_id
      AND d.span_id = s.span_id
      AND d.timestamp = s.timestamp
      AND d.timestamp >= '{{day_start}}'::TIMESTAMPTZ::TIMESTAMP_NS
      AND d.timestamp < ('{{day_start}}'::TIMESTAMPTZ + INTERVAL '1 day')::TIMESTAMP_NS
)