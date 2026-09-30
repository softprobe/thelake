CREATE TABLE IF NOT EXISTS scores (
    score_id VARCHAR NOT NULL,
    timestamp TIMESTAMP_NS NOT NULL,
    trace_id VARCHAR,
    span_id VARCHAR,
    session_id VARCHAR,
    name VARCHAR NOT NULL,
    data_type VARCHAR NOT NULL,
    numeric_value DOUBLE,
    string_value VARCHAR,
    boolean_value BOOLEAN,
    source VARCHAR NOT NULL,
    comment VARCHAR,
    config_id VARCHAR,
    author_id VARCHAR,
    metadata MAP(VARCHAR, VARCHAR),
    tenant_id VARCHAR
);
