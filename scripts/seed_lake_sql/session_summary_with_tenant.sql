CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.session_summary (
  tenant_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ,
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
  PRIMARY KEY (tenant_id, session_id)
);
