CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.tag_index (
  workspace_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  last_seen_ns BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, tag)
);
CREATE INDEX IF NOT EXISTS tag_index_workspace_last_seen
  ON {{schema}}.tag_index (workspace_id, last_seen_ns DESC, tag);
