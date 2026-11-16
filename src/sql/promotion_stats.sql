WITH sampled AS (
    SELECT {{sample_cols}}
    FROM {{table_name}}
    {{where_clause}}
    LIMIT {{sample_limit}}
)
SELECT {{agg_cols}}
FROM sampled
