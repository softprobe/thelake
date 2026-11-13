WITH raw_spans AS MATERIALIZED (
    SELECT
        session_id,
        trace_id,
        span_id,
        parent_span_id,
        timestamp,
        end_timestamp,
        status_code,
        message_type,
        agent_name,
        {{observation_type}} AS observation_type,
        {{input_tokens}} AS input_tokens,
        {{output_tokens}} AS output_tokens,
        {{total_tokens}} AS total_tokens,
        {{total_cost}} AS total_cost,
        {{user_id}} AS user_id,
        {{model_name}} AS model_name
    FROM {{from_table}}
    WHERE {{where_prefix}}{{timestamp_filter}}
      AND COALESCE({{observation_type}}, '') <> 'recording'
),
{{usage_leaf_ctes}}
SELECT
    session_id,
    epoch_ns(MIN(timestamp))::BIGINT AS start_time_ns,
    epoch_ns(MAX(COALESCE(end_timestamp, timestamp)))::BIGINT AS end_time_ns,
    COUNT(DISTINCT span_id) FILTER (WHERE COALESCE(observation_type, '') <> 'http_body')::BIGINT AS observation_count,
    SUM(CASE WHEN status_code = 'ERROR' AND COALESCE(observation_type, '') <> 'http_body' THEN 1 ELSE 0 END)::BIGINT AS error_count,
    SUM(input_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS input_tokens,
    SUM(output_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS output_tokens,
    SUM(usage_total_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS total_tokens,
    SUM(total_cost) AS total_cost,
    {{agent_expr}} AS agent_name,
    arg_min(user_id, timestamp) FILTER (WHERE NULLIF(user_id, '') IS NOT NULL) AS user_id,
    arg_min(model_name, timestamp) FILTER (WHERE NULLIF(model_name, '') IS NOT NULL) AS model_name
FROM marked_spans
GROUP BY session_id
HAVING COUNT(DISTINCT span_id) FILTER (WHERE COALESCE(observation_type, '') <> 'http_body') > 0
