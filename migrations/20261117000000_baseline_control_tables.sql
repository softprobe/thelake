-- Baseline Softprobe Postgres control and operational tables.
-- Target schema is selected via connection search_path (e.g. thelake or thelake_staging_ducklake11).
-- Safe to apply against already-initialized schemas (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS physical_scope (
  physical_scope_id TEXT PRIMARY KEY,
  metadata_path TEXT NOT NULL,
  ducklake_metadata_schema TEXT NOT NULL,
  data_path TEXT NOT NULL,
  catalog_alias TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (metadata_path, catalog_alias, ducklake_metadata_schema, data_path)
);

CREATE TABLE IF NOT EXISTS workspace_scope_binding (
  workspace_id TEXT PRIMARY KEY,
  physical_scope_id TEXT NOT NULL REFERENCES physical_scope(physical_scope_id),
  lifecycle_status TEXT NOT NULL DEFAULT 'active'
    CHECK (lifecycle_status IN ('active', 'deleting', 'deleted')),
  provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS thelake_job_lease (
  job_name TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  holder_id TEXT NOT NULL,
  epoch BIGINT NOT NULL DEFAULT 1,
  lease_until TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (job_name, scope_key)
);

CREATE INDEX IF NOT EXISTS thelake_job_lease_until
  ON thelake_job_lease (lease_until);

CREATE TABLE IF NOT EXISTS maintenance_scope_config (
  scope_key TEXT NOT NULL,
  catalog_alias TEXT NOT NULL,
  metadata_schema TEXT NOT NULL,
  compaction_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  metadata_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  reader_safety_grace_seconds BIGINT NOT NULL DEFAULT 300 CHECK (reader_safety_grace_seconds >= 0),
  lease_job TEXT,
  lease_holder TEXT,
  lease_epoch BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (scope_key, lease_epoch),
  CHECK ((lease_epoch = 0 AND lease_job IS NULL AND lease_holder IS NULL)
      OR (lease_epoch > 0 AND lease_job IS NOT NULL AND lease_holder IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS session_summary (
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
  ON session_summary (workspace_id, start_time_ns DESC, session_id);

CREATE INDEX IF NOT EXISTS session_summary_agent
  ON session_summary (workspace_id, agent_name, start_time_ns DESC, session_id)
  WHERE agent_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS session_summary_errors
  ON session_summary (workspace_id, start_time_ns DESC, session_id)
  WHERE error_count > 0;

CREATE TABLE IF NOT EXISTS session_summary_dirty (
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
  ON session_summary_dirty (claim_until)
  WHERE claim_holder IS NOT NULL;

CREATE OR REPLACE FUNCTION session_summary_dirty_bump_generation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.generation := OLD.generation + 1;
  RETURN NEW;
END;
$$;

DO $$ BEGIN
  LOCK TABLE session_summary_dirty IN SHARE ROW EXCLUSIVE MODE;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'session_summary_dirty_bump_generation'
      AND tgrelid = to_regclass('session_summary_dirty')
      AND NOT tgisinternal
  ) THEN
    CREATE TRIGGER session_summary_dirty_bump_generation
      BEFORE UPDATE ON session_summary_dirty
      FOR EACH ROW EXECUTE FUNCTION session_summary_dirty_bump_generation();
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS tag_index (
  workspace_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  last_seen_ns BIGINT NOT NULL,
  source_mask SMALLINT NOT NULL DEFAULT 3,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, tag)
);

CREATE INDEX IF NOT EXISTS tag_index_workspace_last_seen
  ON tag_index (workspace_id, last_seen_ns DESC, tag);

CREATE TABLE IF NOT EXISTS promotion_specs (
  spec_id TEXT NOT NULL PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS promotion_errors (
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  spec_id TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_table TEXT,
  target_column TEXT NOT NULL,
  session_id TEXT,
  trace_id TEXT,
  span_id TEXT,
  event_name TEXT,
  source_signal TEXT NOT NULL,
  source_path TEXT NOT NULL,
  error_code TEXT NOT NULL,
  error_message TEXT NOT NULL,
  raw_value_preview TEXT
);
