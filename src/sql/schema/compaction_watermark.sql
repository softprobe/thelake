CREATE TABLE IF NOT EXISTS {{schema}}.compaction_watermark (
  scope_key TEXT NOT NULL,
  table_name TEXT NOT NULL,
  watermark TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (scope_key, table_name)
);
