WITH trace_base AS MATERIALIZED (
    SELECT
        trace_id,
        span_id,
        parent_span_id,
        {{session_id_expr}} AS session_id,
        message_type,
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
non_leaf_usage_spans AS (
    SELECT DISTINCT trace_id, ancestor_span_id AS span_id
    FROM (
        WITH RECURSIVE usage_ancestors(trace_id, ancestor_span_id, depth, visited) AS (
            SELECT
                c.trace_id,
                c.parent_span_id AS ancestor_span_id,
                1 AS depth,
                [c.span_id, c.parent_span_id] AS visited
            FROM trace_base c
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
            JOIN trace_base p
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
trace_marked AS (
    SELECT
        s.trace_id,
        s.session_id,
        s.message_type,
        s.timestamp,
        s.end_timestamp,
        s.status_code,
        s.input_tokens,
        s.output_tokens,
        s.total_tokens,
        s.total_cost,
        s.user_id,
        ((s.input_tokens IS NOT NULL OR s.output_tokens IS NOT NULL OR s.total_tokens IS NOT NULL)
         AND nl.span_id IS NULL) AS is_usage_leaf
    FROM trace_base s
    LEFT JOIN non_leaf_usage_spans nl
      ON nl.trace_id = s.trace_id
     AND nl.span_id = s.span_id
)
SELECT
    trace_id,
    any_value(session_id) AS session_id,
    any_value(message_type) AS name,
    MIN(timestamp) AS start_time,
    MAX(COALESCE(end_timestamp, timestamp)) AS end_time,
    COUNT(*)::BIGINT AS span_count,
    SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::BIGINT AS error_count,
    SUM(input_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS input_tokens,
    SUM(output_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS output_tokens,
    SUM(COALESCE(total_tokens, COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0))) FILTER (WHERE is_usage_leaf)::BIGINT AS total_tokens,
    SUM(total_cost) AS total_cost,
    any_value(user_id) AS user_id
FROM trace_marked
GROUP BY trace_id
