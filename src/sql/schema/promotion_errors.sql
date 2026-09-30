CREATE TABLE IF NOT EXISTS {{qualified_table}} (
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
