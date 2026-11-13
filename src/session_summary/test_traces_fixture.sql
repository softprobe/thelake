CREATE TABLE traces (
    session_id VARCHAR,
    span_id VARCHAR,
    timestamp TIMESTAMP_NS,
    end_timestamp TIMESTAMP_NS,
    status_code VARCHAR,
    message_type VARCHAR,
    agent_name VARCHAR,
    observation_type VARCHAR,
    input_tokens BIGINT,
    output_tokens BIGINT,
    total_tokens BIGINT,
    total_cost DOUBLE,
    user_id VARCHAR,
    model_name VARCHAR,
    record_date DATE,
    trace_id VARCHAR DEFAULT 'tr_default',
    parent_span_id VARCHAR DEFAULT NULL
);
