SELECT source_mask
FROM {{schema_quoted}}.tag_index
WHERE workspace_id = $1::text
  AND tag = $2::text
