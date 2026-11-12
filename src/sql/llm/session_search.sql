WITH session_base AS MATERIALIZED (
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
non_leaf_usage_spans AS (
    SELECT DISTINCT trace_id, ancestor_span_id AS span_id
    FROM (
        WITH RECURSIVE usage_ancestors(trace_id, ancestor_span_id, depth, visited) AS (
            SELECT
                c.trace_id,
                c.parent_span_id AS ancestor_span_id,
                1 AS depth,
                [c.span_id, c.parent_span_id] AS visited
            FROM session_base c
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
            JOIN session_base p
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
session_marked AS (
    SELECT
        s.*,
        ((s.input_tokens IS NOT NULL OR s.output_tokens IS NOT NULL OR s.total_tokens IS NOT NULL)
         AND nl.span_id IS NULL) AS is_usage_leaf
    FROM session_base s
    LEFT JOIN non_leaf_usage_spans nl
      ON nl.trace_id = s.trace_id
     AND nl.span_id = s.span_id
)
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
        SUM(COALESCE(total_tokens, COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0))) FILTER (WHERE is_usage_leaf)::BIGINT AS total_tokens,
        SUM(total_cost) AS total_cost,
        COALESCE(
            NULLIF(arg_min(agent_attr, timestamp) FILTER (WHERE NULLIF(agent_attr, '') IS NOT NULL), ''),
            arg_min(message_type, timestamp) FILTER (WHERE obs_type = 'agent')
        ) AS agent_name,
        list(DISTINCT user_id) AS user_ids,
        list(DISTINCT model_name) AS models
    FROM session_marked
    GROUP BY resolved_session_id
    {{having_sql}}
){{cursor_sql}}
ORDER BY {{order_sql}}
LIMIT {{fetch}}
