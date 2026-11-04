CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.capture_session_mappings (
  workspace_id TEXT NOT NULL,
  capture_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (workspace_id, capture_id, trace_id)
);

CREATE TABLE IF NOT EXISTS {{schema}}.pending_capture_spans (
  workspace_id TEXT NOT NULL,
  capture_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  span_id TEXT NOT NULL,
  session_id TEXT,
  span_json TEXT NOT NULL,
  byte_size BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  lease_token TEXT,
  lease_until TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, capture_id, trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS pending_capture_spans_ready_idx
  ON {{schema}}.pending_capture_spans (workspace_id, capture_id, expires_at)
  WHERE delivered_at IS NULL;

CREATE INDEX IF NOT EXISTS pending_capture_spans_expiry_idx
  ON {{schema}}.pending_capture_spans (expires_at);

CREATE INDEX IF NOT EXISTS capture_session_mappings_expiry_idx
  ON {{schema}}.capture_session_mappings (expires_at);
