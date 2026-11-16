WITH has_index AS (
  SELECT EXISTS (
    SELECT 1 FROM {{schema_quoted}}.tag_index WHERE workspace_id = $1::text
  ) AS populated
),
matched AS (
  SELECT tag
  FROM {{schema_quoted}}.tag_index
  WHERE workspace_id = $1::text
    AND last_seen_ns >= $2::bigint
  ORDER BY tag ASC
  LIMIT $3::bigint
)
SELECT has_index.populated, matched.tag
FROM has_index
LEFT JOIN matched ON TRUE
