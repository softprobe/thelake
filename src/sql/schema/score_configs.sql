CREATE TABLE IF NOT EXISTS score_configs (
    config_id VARCHAR NOT NULL,
    timestamp TIMESTAMP_NS NOT NULL,
    name VARCHAR NOT NULL,
    data_type VARCHAR NOT NULL,
    description VARCHAR,
    min_value DOUBLE,
    max_value DOUBLE,
    categories VARCHAR,
    author_id VARCHAR,
    metadata MAP(VARCHAR, VARCHAR),
    workspace_id VARCHAR
);
