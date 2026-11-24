CREATE UNIQUE INDEX IF NOT EXISTS promotion_specs_one_active_per_target_idx
  ON promotion_specs (target_kind, COALESCE(target_tables, target_table, ''))
  WHERE status = 'active';
