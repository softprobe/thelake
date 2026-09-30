CREATE TABLE IF NOT EXISTS {{qualified_table}} (
  spec_id TEXT NOT NULL{{spec_id_constraint}},
  spec_version TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_table TEXT,
  target_tables TEXT,
  business_version BIGINT,
  manifest_json TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_by TEXT,
  status TEXT NOT NULL
);
