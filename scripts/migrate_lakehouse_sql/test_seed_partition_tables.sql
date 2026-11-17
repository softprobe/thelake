ATTACH ':memory:' AS src_db;
ATTACH ':memory:' AS dst_db;
CREATE TABLE src_db.traces (
    trace_id VARCHAR,
    timestamp TIMESTAMP_NS,
    attributes MAP(VARCHAR, VARCHAR)
);
CREATE TABLE dst_db.traces (
    trace_id VARCHAR,
    timestamp TIMESTAMP_NS,
    attributes VARIANT
);
INSERT INTO src_db.traces VALUES
    ('t1', '2026-11-16T01:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, MAP {'k': 'v1'}),
    ('t2', '2026-11-16T15:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, MAP {'k': 'v2'});
INSERT INTO dst_db.traces VALUES
    ('live_1', '2026-11-16T16:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, '{"k":"live"}'::JSON::VARIANT);
