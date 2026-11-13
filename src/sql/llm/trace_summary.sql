WITH raw_spans AS MATERIALIZED (
    SELECT
        trace_id,
        span_id,
        parent_span_id,
        {{session_id_expr}} AS session_id,
        message_type,
        COALESCE({{obs_type_expr}}, 'span') AS obs_type,
        timestamp,
        end_timestamp,
        status_code,
        {{input_tokens_expr}} AS input_tokens,
        {{output_tokens_expr}} AS output_tokens,
        {{total_tokens_expr}} AS total_tokens,
        {{total_cost_expr}} AS total_cost,
        {{user_id_expr}} AS user_id
    FROM traces
    WHERE {{identity_sql}}
      AND {{timestamp_filter}}
),
{{usage_leaf_ctes}}
SELECT
    trace_id,
    any_value(session_id) AS session_id,
    COALESCE(
        any_value(message_type) FILTER (WHERE obs_type <> 'http_body'),
        any_value(message_type)
    ) AS name,
    MIN(timestamp) AS start_time,
    MAX(COALESCE(end_timestamp, timestamp)) AS end_time,
    COUNT(*) FILTER (WHERE obs_type <> 'http_body')::BIGINT AS span_count,
    SUM(CASE WHEN status_code = 'ERROR' AND obs_type <> 'http_body' THEN 1 ELSE 0 END)::BIGINT AS error_count,
    SUM(input_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS input_tokens,
    SUM(output_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS output_tokens,
    SUM(usage_total_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS total_tokens,
    SUM(total_cost) AS total_cost,
    any_value(user_id) AS user_id
FROM marked_spans
GROUP BY trace_id
