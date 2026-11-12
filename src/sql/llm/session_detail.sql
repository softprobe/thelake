WITH session_spans AS MATERIALIZED (
    SELECT {{projection}}
    FROM traces
    WHERE {{session_predicate}}
      AND {{exclude_recording}}
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
            FROM session_spans c
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
            JOIN session_spans p
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
session_spans_marked AS (
    SELECT
        s.*,
        ((s.input_tokens IS NOT NULL OR s.output_tokens IS NOT NULL OR s.total_tokens IS NOT NULL)
         AND nl.span_id IS NULL) AS is_usage_leaf
    FROM session_spans s
    LEFT JOIN non_leaf_usage_spans nl
      ON nl.trace_id = s.trace_id
     AND nl.span_id = s.span_id
),
session_scores AS MATERIALIZED (
    SELECT {{score_columns}}
    FROM scores
    WHERE {{timestamp_filter}}
      AND (session_id = {{session_id_literal}}
        OR trace_id IN (SELECT DISTINCT trace_id FROM session_spans)
        OR span_id IN (SELECT span_id FROM session_spans))
),
score_aggregate AS (
    SELECT COALESCE(to_json(list(struct_pack(
        score_id := score_id,
        "timestamp" := timestamp,
        trace_id := trace_id,
        span_id := span_id,
        session_id := session_id,
        name := name,
        data_type := data_type,
        numeric_value := numeric_value,
        string_value := string_value,
        boolean_value := boolean_value,
        source := source,
        comment := comment,
        config_id := config_id,
        author_id := author_id,
        metadata := metadata
    ) ORDER BY timestamp DESC, score_id DESC)), '[]') AS session_scores
    FROM session_scores
),
session_aggregate AS (
    SELECT
        first(score_aggregate.session_scores) AS session_scores,
        COUNT(DISTINCT trace_id) FILTER (WHERE span_type <> 'http_body')::BIGINT AS session_trace_count,
        COUNT(*) FILTER (WHERE span_type <> 'http_body')::BIGINT AS session_span_count,
        SUM(input_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS session_input_tokens,
        SUM(output_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS session_output_tokens,
        SUM(COALESCE(total_tokens, COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0))) FILTER (WHERE is_usage_leaf)::BIGINT AS session_total_tokens,
        SUM(total_cost) AS session_total_cost,
        list(DISTINCT user_id) AS session_user_ids
    FROM session_spans_marked
    CROSS JOIN score_aggregate
)
SELECT session_spans.*, session_aggregate.*
FROM session_spans
CROSS JOIN session_aggregate
ORDER BY start_time DESC, span_id DESC
