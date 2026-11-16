INSERT INTO {{schema_quoted}}.tag_index
  (workspace_id, tag, last_seen_ns, source_mask, updated_at)
SELECT $1::text, input.tag, input.last_seen_ns, input.source_mask, clock_timestamp()
FROM UNNEST($2::text[], $3::bigint[], $4::smallint[])
  AS input(tag, last_seen_ns, source_mask)
ON CONFLICT (workspace_id, tag) DO UPDATE SET
  last_seen_ns = GREATEST(tag_index.last_seen_ns, EXCLUDED.last_seen_ns),
  source_mask = tag_index.source_mask | EXCLUDED.source_mask,
  updated_at = EXCLUDED.updated_at
