UPDATE "{{schema}}".promotion_specs
SET status = 'inactive'
WHERE status = 'active'
  AND target_kind = $1
  AND COALESCE(target_tables, target_table, '') = $2
  AND spec_id <> $3;
