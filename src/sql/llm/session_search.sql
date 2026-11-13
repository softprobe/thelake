WITH raw_spans AS MATERIALIZED (
    SELECT
        {{session_id_expr}} AS resolved_session_id,
        trace_id,
        span_id,
        parent_span_id,
        timestamp,
        end_timestamp,
        status_code,
        message_type,
        {{obs_type_expr}} AS obs_type,
        {{input_tokens_expr}} AS input_tokens,
        {{output_tokens_expr}} AS output_tokens,
        {{total_tokens_expr}} AS total_tokens,
        {{total_cost_expr}} AS total_cost,
        {{agent_attr_expr}} AS agent_attr,
        {{user_id_expr}} AS user_id,
        {{model_name_expr}} AS model_name,
        COALESCE(CAST(attributes['sp.metadata.opencode.parentSessionID'] AS VARCHAR), '') AS parent_session_id
    FROM traces
    WHERE {{identity_sql}}
      AND {{timestamp_filter}}
),
{{usage_leaf_ctes}}
SELECT * FROM (
    SELECT
        resolved_session_id AS session_id,
        MIN(timestamp) AS start_time,
        MAX(COALESCE(end_timestamp, timestamp)) AS end_time,
        date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp)))::BIGINT AS duration_ms,
        COUNT(DISTINCT trace_id) FILTER (WHERE obs_type <> 'http_body')::BIGINT AS trace_count,
        COUNT(*) FILTER (WHERE obs_type <> 'http_body')::BIGINT AS observation_count,
        SUM(CASE WHEN status_code = 'ERROR' AND obs_type <> 'http_body' THEN 1 ELSE 0 END)::BIGINT AS error_count,
        SUM(input_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS input_tokens,
        SUM(output_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS output_tokens,
        SUM(usage_total_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS total_tokens,
        SUM(total_cost) AS total_cost,
        COALESCE(
            NULLIF(arg_min(agent_attr, timestamp) FILTER (WHERE NULLIF(agent_attr, '') IS NOT NULL), ''),
            arg_min(message_type, timestamp) FILTER (WHERE obs_type = 'agent')
        ) AS agent_name,
        list(DISTINCT user_id) AS user_ids,
        list(DISTINCT model_name) AS models
    FROM marked_spans
    GROUP BY resolved_session_id
    {{having_sql}}
){{cursor_sql}}
ORDER BY {{order_sql}}
LIMIT {{fetch}}
