CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.session_summary (
  workspace_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  start_time_ns BIGINT NOT NULL,
  end_time_ns BIGINT,
  observation_count BIGINT NOT NULL DEFAULT 0,
  error_count BIGINT NOT NULL DEFAULT 0,
  input_tokens BIGINT,
  output_tokens BIGINT,
  total_tokens BIGINT,
  total_cost DOUBLE PRECISION,
  agent_name TEXT,
  user_id TEXT,
  model_name TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (workspace_id, session_id)
);
CREATE INDEX IF NOT EXISTS session_summary_recent
  ON {{schema}}.session_summary (workspace_id, start_time_ns DESC, session_id);
CREATE INDEX IF NOT EXISTS session_summary_agent
  ON {{schema}}.session_summary (workspace_id, agent_name, start_time_ns DESC, session_id)
  WHERE agent_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS session_summary_errors
  ON {{schema}}.session_summary (workspace_id, start_time_ns DESC, session_id)
  WHERE error_count > 0;
CREATE TABLE IF NOT EXISTS {{schema}}.session_summary_dirty (
  workspace_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  min_ts_ns BIGINT NOT NULL,
  max_ts_ns BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  generation BIGINT NOT NULL DEFAULT 1,
  claim_holder TEXT,
  claim_until TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, session_id)
);
CREATE INDEX IF NOT EXISTS session_summary_dirty_claim
  ON {{schema}}.session_summary_dirty (claim_until)
  WHERE claim_holder IS NOT NULL;
