INSERT INTO {{schema_quoted}}.tag_index
  (workspace_id, tag, last_seen_ns, updated_at)
SELECT $1::text, input.tag, input.last_seen_ns, clock_timestamp()
FROM UNNEST($2::text[], $3::bigint[])
  AS input(tag, last_seen_ns)
ON CONFLICT (workspace_id, tag) DO UPDATE SET
  last_seen_ns = GREATEST(tag_index.last_seen_ns, EXCLUDED.last_seen_ns),
  updated_at = EXCLUDED.updated_at
