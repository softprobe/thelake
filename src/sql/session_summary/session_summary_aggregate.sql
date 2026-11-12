WITH base_spans AS MATERIALIZED (
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
non_leaf_usage_spans AS (
    SELECT DISTINCT trace_id, ancestor_span_id AS span_id
    FROM (
        WITH RECURSIVE usage_ancestors(trace_id, ancestor_span_id, depth, visited) AS (
            SELECT
                c.trace_id,
                c.parent_span_id AS ancestor_span_id,
                1 AS depth,
                [c.span_id, c.parent_span_id] AS visited
            FROM base_spans c
            WHERE (c.input_tokens IS NOT NULL OR c.output_tokens IS NOT NULL OR c.total_tokens IS NOT NULL)
              AND c.parent_span_id IS NOT NULL
              AND c.parent_span_id <> ''
              AND c.parent_span_id <> c.span_id
            UNION ALL
            SELECT
                a.trace_id,
                p.parent_span_id AS ancestor_span_id,
                a.depth + 1 AS depth,
                list_append(a.visited, p.parent_span_id) AS visited
            FROM usage_ancestors a
            JOIN base_spans p
              ON p.trace_id = a.trace_id
             AND p.span_id = a.ancestor_span_id
            WHERE a.depth < 64
              AND p.parent_span_id IS NOT NULL
              AND p.parent_span_id <> ''
              AND NOT list_contains(a.visited, p.parent_span_id)
        )
        SELECT trace_id, ancestor_span_id FROM usage_ancestors
    )
),
marked_spans AS (
    SELECT
        s.session_id,
        s.span_id,
        s.timestamp,
        s.end_timestamp,
        s.status_code,
        s.message_type,
        s.agent_name,
        s.observation_type,
        s.input_tokens,
        s.output_tokens,
        s.total_tokens,
        s.total_cost,
        s.user_id,
        s.model_name,
        ((s.input_tokens IS NOT NULL OR s.output_tokens IS NOT NULL OR s.total_tokens IS NOT NULL)
         AND nl.span_id IS NULL) AS is_usage_leaf
    FROM base_spans s
    LEFT JOIN non_leaf_usage_spans nl
      ON nl.trace_id = s.trace_id
     AND nl.span_id = s.span_id
)
SELECT
    session_id,
    epoch_ns(MIN(timestamp))::BIGINT AS start_time_ns,
    epoch_ns(MAX(COALESCE(end_timestamp, timestamp)))::BIGINT AS end_time_ns,
    COUNT(DISTINCT span_id) FILTER (WHERE COALESCE(observation_type, '') <> 'http_body')::BIGINT AS observation_count,
    SUM(CASE WHEN status_code = 'ERROR' AND COALESCE(observation_type, '') <> 'http_body' THEN 1 ELSE 0 END)::BIGINT AS error_count,
    SUM(input_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS input_tokens,
    SUM(output_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS output_tokens,
    SUM(COALESCE(total_tokens, COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0))) FILTER (WHERE is_usage_leaf)::BIGINT AS total_tokens,
    SUM(total_cost) AS total_cost,
    {{agent_expr}} AS agent_name,
    arg_min(user_id, timestamp) FILTER (WHERE NULLIF(user_id, '') IS NOT NULL) AS user_id,
    arg_min(model_name, timestamp) FILTER (WHERE NULLIF(model_name, '') IS NOT NULL) AS model_name
FROM marked_spans
GROUP BY session_id
HAVING COUNT(DISTINCT span_id) FILTER (WHERE COALESCE(observation_type, '') <> 'http_body') > 0
