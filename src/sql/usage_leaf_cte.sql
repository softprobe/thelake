deduped_spans AS (
    SELECT r.*
    FROM raw_spans r
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY r.{{scope_col}}, r.trace_id, r.span_id
        ORDER BY r.{{dedup_order_col}} DESC
    ) = 1
),
non_leaf_usage_spans AS (
    SELECT DISTINCT scope_key, trace_id, ancestor_span_id AS span_id
    FROM (
        WITH RECURSIVE usage_ancestors(scope_key, trace_id, ancestor_span_id, depth, visited) AS (
            SELECT
                c.{{scope_col}} AS scope_key,
                c.trace_id,
                c.parent_span_id AS ancestor_span_id,
                1 AS depth,
                [c.span_id, c.parent_span_id] AS visited
            FROM deduped_spans c
            WHERE (c.input_tokens IS NOT NULL OR c.output_tokens IS NOT NULL OR c.total_tokens IS NOT NULL)
              AND c.parent_span_id IS NOT NULL
              AND c.parent_span_id <> ''
              AND c.parent_span_id <> c.span_id
            UNION ALL
            SELECT
                a.scope_key,
                a.trace_id,
                p.parent_span_id AS ancestor_span_id,
                a.depth + 1 AS depth,
                list_append(a.visited, p.parent_span_id) AS visited
            FROM usage_ancestors a
            JOIN deduped_spans p
              ON p.{{scope_col}} = a.scope_key
             AND p.trace_id = a.trace_id
             AND p.span_id = a.ancestor_span_id
            WHERE a.depth < 64
              AND p.parent_span_id IS NOT NULL
              AND p.parent_span_id <> ''
              AND NOT list_contains(a.visited, p.parent_span_id)
        )
        SELECT scope_key, trace_id, ancestor_span_id FROM usage_ancestors
    )
),
marked_spans AS (
    SELECT
        s.*,
        ((s.input_tokens IS NOT NULL OR s.output_tokens IS NOT NULL OR s.total_tokens IS NOT NULL)
         AND nl.span_id IS NULL) AS is_usage_leaf,
        COALESCE(s.total_tokens, COALESCE(s.input_tokens, 0) + COALESCE(s.output_tokens, 0)) AS usage_total_tokens
    FROM deduped_spans s
    LEFT JOIN non_leaf_usage_spans nl
      ON nl.scope_key = s.{{scope_col}}
     AND nl.trace_id = s.trace_id
     AND nl.span_id = s.span_id
)
