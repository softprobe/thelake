WITH raw_spans AS MATERIALIZED (
    SELECT {{projection}}
    FROM traces
    WHERE {{session_predicate}}
      AND {{exclude_recording}}
      AND {{timestamp_filter}}
),
{{usage_leaf_ctes}},
session_scores AS MATERIALIZED (
    SELECT {{score_columns}}
    FROM scores
    WHERE {{timestamp_filter}}
      AND (session_id = {{session_id_literal}}
        OR trace_id IN (SELECT DISTINCT trace_id FROM deduped_spans)
        OR span_id IN (SELECT span_id FROM deduped_spans))
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
        SUM(usage_total_tokens) FILTER (WHERE is_usage_leaf)::BIGINT AS session_total_tokens,
        SUM(total_cost) AS session_total_cost,
        list(DISTINCT user_id) AS session_user_ids
    FROM marked_spans
    CROSS JOIN score_aggregate
)
SELECT deduped_spans.*, session_aggregate.*
FROM deduped_spans
CROSS JOIN session_aggregate
ORDER BY start_time DESC, span_id DESC
