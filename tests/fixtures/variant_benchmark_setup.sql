CREATE TABLE IF NOT EXISTS bench_map (
    id BIGINT NOT NULL,
    ts TIMESTAMP_NS NOT NULL,
    attributes MAP(VARCHAR, VARCHAR),
    events VARCHAR,
    sp_user_id VARCHAR
);
CREATE TABLE IF NOT EXISTS bench_variant (
    id BIGINT NOT NULL,
    ts TIMESTAMP_NS NOT NULL,
    attributes VARIANT,
    events VARIANT,
    sp_user_id VARCHAR
);
