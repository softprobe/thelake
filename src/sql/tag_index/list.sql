SELECT tag
FROM {{schema_quoted}}.tag_index
WHERE workspace_id = $1::text
ORDER BY tag ASC
LIMIT $2::bigint
