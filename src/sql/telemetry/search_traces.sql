WITH window_spans AS (
    SELECT
        trace_id,
        span_id,
        parent_span_id,
        session_id,
        app_id,
        agent_id,
        agent_name,
        message_type,
        span_kind,
        timestamp,
        end_timestamp,
        status_code,
        status_message,
        http_request_method,
        http_request_path,
        http_response_status_code,
        {{observation_type}} AS observation_type
    FROM traces
    WHERE {{where_prefix}}{{timestamp_filter}}
      AND COALESCE({{observation_type}}, '') <> 'recording'
),
grouped_traces AS (
    SELECT
        trace_id AS id,
        'trace' AS kind,
        NULLIF(arg_max(session_id, timestamp) FILTER (WHERE session_id <> ''), '') AS session_id,
        trace_id,
        MIN(timestamp) AS start_time,
        MAX(COALESCE(end_timestamp, timestamp)) AS end_time,
        COUNT(DISTINCT span_id) FILTER (WHERE COALESCE(observation_type, '') <> 'http_body')::BIGINT AS span_count,
        SUM(CASE WHEN (status_code = 'ERROR' OR http_response_status_code >= 500) AND COALESCE(observation_type, '') <> 'http_body' THEN 1 ELSE 0 END)::BIGINT AS error_count,
        CASE WHEN SUM(CASE WHEN (status_code = 'ERROR' OR http_response_status_code >= 500) AND COALESCE(observation_type, '') <> 'http_body' THEN 1 ELSE 0 END) > 0 THEN 'ERROR' ELSE 'OK' END AS status_code,
        date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp))) AS duration_ms,
        string_agg(DISTINCT app_id, ',') AS services,
        COALESCE(
            arg_min(message_type, timestamp) FILTER (WHERE parent_span_id IS NULL OR parent_span_id = ''),
            arg_min(message_type, timestamp)
        ) AS name,
        COALESCE(
            arg_min(http_request_method, timestamp) FILTER (WHERE (parent_span_id IS NULL OR parent_span_id = '') AND http_request_method IS NOT NULL AND http_request_method <> ''),
            arg_min(http_request_method, timestamp) FILTER (WHERE http_request_method IS NOT NULL AND http_request_method <> '')
        ) AS http_request_method,
        COALESCE(
            arg_min(http_request_path, timestamp) FILTER (WHERE (parent_span_id IS NULL OR parent_span_id = '') AND http_request_path IS NOT NULL AND http_request_path <> ''),
            arg_min(http_request_path, timestamp) FILTER (WHERE http_request_path IS NOT NULL AND http_request_path <> '')
        ) AS entry_path,
        arg_min(http_response_status_code, timestamp) FILTER (WHERE http_response_status_code IS NOT NULL) AS http_response_status_code,
        arg_min(agent_id, timestamp) FILTER (WHERE agent_id IS NOT NULL AND agent_id <> '') AS agent_id,
        arg_min(agent_name, timestamp) FILTER (WHERE agent_name IS NOT NULL AND agent_name <> '') AS agent_name,
        arg_max(status_message, timestamp) FILTER (WHERE status_message IS NOT NULL AND status_message <> '') AS last_error
    FROM window_spans
    GROUP BY trace_id
    HAVING {{having_filter}}
)
SELECT
    id,
    kind,
    session_id,
    trace_id,
    start_time,
    end_time,
    span_count,
    error_count,
    duration_ms,
    services,
    name,
    http_request_method,
    entry_path,
    http_response_status_code,
    agent_id,
    agent_name,
    last_error
FROM grouped_traces
WHERE {{cursor_filter}}
{{order_sql}}
LIMIT {{fetch_limit}}
