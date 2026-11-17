ATTACH ':memory:' AS src_lake;
ATTACH ':memory:' AS dst_lake;
CREATE SCHEMA src_lake.thelake;
CREATE SCHEMA dst_lake.thelake_v2;
CREATE TABLE src_lake.thelake.traces (
  trace_id VARCHAR,
  span_id VARCHAR,
  timestamp TIMESTAMP_NS,
  attributes MAP(VARCHAR, VARCHAR)
);
CREATE TABLE dst_lake.thelake_v2.traces (
  trace_id VARCHAR,
  span_id VARCHAR,
  timestamp TIMESTAMP_NS,
  attributes VARIANT
);
INSERT INTO src_lake.thelake.traces VALUES
  ('t1', 's1', '2026-11-16T10:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, MAP {'k': 'v'}),
  ('t2', 's2', '2026-11-16T11:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, MAP {});
