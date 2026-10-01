CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.physical_scope (
  physical_scope_id TEXT PRIMARY KEY,
  metadata_path TEXT NOT NULL,
  ducklake_metadata_schema TEXT NOT NULL,
  data_path TEXT NOT NULL,
  catalog_alias TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (metadata_path, catalog_alias, ducklake_metadata_schema, data_path)
);
CREATE TABLE IF NOT EXISTS {{schema}}.workspace_scope_binding (
  workspace_id TEXT PRIMARY KEY,
  physical_scope_id TEXT NOT NULL REFERENCES {{schema}}.physical_scope(physical_scope_id),
  provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS {{schema}}.thelake_job_lease (
  job_name TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  holder_id TEXT NOT NULL,
  epoch BIGINT NOT NULL DEFAULT 1,
  lease_until TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (job_name, scope_key)
);
CREATE INDEX IF NOT EXISTS thelake_job_lease_until
  ON {{schema}}.thelake_job_lease (lease_until);
CREATE TABLE IF NOT EXISTS {{schema}}.maintenance_scope_config (
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
