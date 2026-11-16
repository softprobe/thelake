CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.tag_index (
  workspace_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  last_seen_ns BIGINT NOT NULL,
  source_mask SMALLINT NOT NULL DEFAULT 3,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, tag)
);
ALTER TABLE {{schema}}.tag_index
  ADD COLUMN IF NOT EXISTS source_mask SMALLINT NOT NULL DEFAULT 3;
CREATE INDEX IF NOT EXISTS tag_index_workspace_last_seen
  ON {{schema}}.tag_index (workspace_id, last_seen_ns DESC, tag);
