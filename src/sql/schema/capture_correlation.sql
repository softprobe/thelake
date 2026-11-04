CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.capture_session_mappings (
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workspace_id, agent_id, trace_id)
);

CREATE TABLE IF NOT EXISTS {{schema}}.pending_capture_spans (
  workspace_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  span_id TEXT NOT NULL,
  session_id TEXT,
  span_json TEXT NOT NULL,
  byte_size BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  lease_token TEXT,
  lease_until TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, agent_id, trace_id, span_id)
);

-- Pending rows from the old capture-ID schema cannot be mapped to an Agent.
-- Drop those short-lived joins once, then reuse the renamed key column.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = '{{schema}}' AND table_name = 'capture_session_mappings' AND column_name = 'capture_id'
  ) THEN
    TRUNCATE {{schema}}.capture_session_mappings;
    ALTER TABLE {{schema}}.capture_session_mappings RENAME COLUMN capture_id TO agent_id;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = '{{schema}}' AND table_name = 'pending_capture_spans' AND column_name = 'capture_id'
  ) THEN
    TRUNCATE {{schema}}.pending_capture_spans;
    ALTER TABLE {{schema}}.pending_capture_spans RENAME COLUMN capture_id TO agent_id;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS pending_capture_spans_ready_idx
  ON {{schema}}.pending_capture_spans (workspace_id, agent_id, expires_at)
  WHERE delivered_at IS NULL;

CREATE INDEX IF NOT EXISTS pending_capture_spans_expiry_idx
  ON {{schema}}.pending_capture_spans (expires_at);

CREATE INDEX IF NOT EXISTS capture_session_mappings_expiry_idx
  ON {{schema}}.capture_session_mappings (expires_at);
