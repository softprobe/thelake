-- One SQL-owned maintenance pass for the attached physical DuckLake scope.
{{otlp_layout_sql}}
SET preserve_insertion_order = getvariable('thelake_otlp_preserve_insertion_order');
SET VARIABLE thelake_maintenance_scope_key = '{{scope_key}}';
SET VARIABLE thelake_maintenance_pass_started_at = (SELECT current_timestamp);

CREATE OR REPLACE TEMP TABLE thelake_maintenance_context AS
SELECT * FROM __thelake_registry."{{registry_schema}}".maintenance_scope_config
WHERE scope_key = getvariable('thelake_maintenance_scope_key')
  AND lease_epoch = {{lease_epoch}};

SET VARIABLE thelake_catalog_alias = (SELECT catalog_alias
  FROM thelake_maintenance_context);
SET VARIABLE thelake_metadata_schema = (SELECT metadata_schema
  FROM thelake_maintenance_context);
SET VARIABLE thelake_compaction_enabled = (SELECT compaction_enabled
  FROM thelake_maintenance_context);
SET VARIABLE thelake_metadata_enabled = (SELECT metadata_enabled
  FROM thelake_maintenance_context);
SET VARIABLE thelake_reader_safety_grace_seconds = (SELECT reader_safety_grace_seconds
  FROM thelake_maintenance_context);
SET VARIABLE thelake_lease_job = (SELECT lease_job
  FROM thelake_maintenance_context);
SET VARIABLE thelake_lease_holder = (SELECT lease_holder
  FROM thelake_maintenance_context);
SET VARIABLE thelake_lease_epoch = (SELECT lease_epoch
  FROM thelake_maintenance_context);

CREATE TABLE IF NOT EXISTS __thelake_registry."{{registry_schema}}".compaction_watermark (
  scope_key TEXT NOT NULL,
  table_name TEXT NOT NULL,
  watermark TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (scope_key, table_name)
);
CREATE TABLE IF NOT EXISTS __thelake_registry."{{registry_schema}}".maintenance_outcome (
  scope_key TEXT NOT NULL,
  pass_started_at TIMESTAMPTZ NOT NULL,
  table_name TEXT NOT NULL,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  files_processed BIGINT NOT NULL DEFAULT 0,
  files_created BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (scope_key, table_name, action)
);
CREATE OR REPLACE TEMP TABLE thelake_maintenance_result (
  table_name VARCHAR,
  action VARCHAR,
  status VARCHAR,
  files_processed BIGINT,
  files_created BIGINT
);
CREATE OR REPLACE TEMP TABLE thelake_maintenance_tables AS
SELECT requested.table_name, lake.table_name IS NOT NULL AS table_exists
FROM (VALUES ('traces'), ('logs'), ('scores')) AS requested(table_name)
LEFT JOIN duckdb_tables() AS lake
  ON lake.database_name = getvariable('thelake_catalog_alias')
 AND lake.schema_name = getvariable('thelake_metadata_schema')
 AND lake.table_name = requested.table_name;

-- The output layout (day partition, ordering, file size, compression, row group)
-- is persisted on each table by the shared writer layout.
SELECT CASE WHEN (getvariable('thelake_lease_epoch') = 0 AND NOT EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = 'physical_scope_maintenance'
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND lease_until > now()
)) OR EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = getvariable('thelake_lease_job')
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND holder_id = getvariable('thelake_lease_holder')
    AND epoch = getvariable('thelake_lease_epoch')
    AND lease_until > now()
) THEN TRUE ELSE error('maintenance lease lost') END;

DELETE FROM __thelake_registry."{{registry_schema}}".maintenance_scope_config AS config
WHERE config.scope_key = getvariable('thelake_maintenance_scope_key')
  AND config.lease_epoch > 0
  AND config.lease_epoch <> getvariable('thelake_lease_epoch')
  AND NOT EXISTS (
    SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease AS lease
    WHERE lease.job_name = config.lease_job AND lease.scope_key = config.scope_key
      AND lease.holder_id = config.lease_holder AND lease.epoch = config.lease_epoch
      AND lease.lease_until > now()
  );

SET VARIABLE thelake_merge_sql = (
  SELECT coalesce(string_agg(
    format(
      'SELECT ''{}''::VARCHAR AS table_name, coalesce(sum(files_processed), 0)::BIGINT AS files_processed, coalesce(sum(files_created), 0)::BIGINT AS files_created FROM ducklake_merge_adjacent_files(getvariable(''thelake_catalog_alias''), ''{}'', schema => getvariable(''thelake_metadata_schema''), newer_than => (TIMESTAMP ''{}'' AT TIME ZONE ''UTC''))',
      tables.table_name, tables.table_name,
      timezone('UTC', CASE WHEN getvariable('thelake_compaction_enabled') THEN coalesce(
        watermark.watermark, TIMESTAMPTZ '0001-01-01 00:00:00+00')
      ELSE current_timestamp + INTERVAL '100 years' END)
    ),
    ' UNION ALL '
  ), 'SELECT NULL::VARCHAR AS table_name, 0::BIGINT AS files_processed, 0::BIGINT AS files_created WHERE FALSE')
  FROM thelake_maintenance_tables AS tables
  LEFT JOIN __thelake_registry."{{registry_schema}}".compaction_watermark AS watermark
    ON watermark.scope_key = getvariable('thelake_maintenance_scope_key')
   AND watermark.table_name = tables.table_name
  WHERE getvariable('thelake_compaction_enabled')
    AND tables.table_exists
);
CREATE OR REPLACE TEMP TABLE thelake_merge_results AS
SELECT * FROM query(getvariable('thelake_merge_sql'));

INSERT INTO thelake_maintenance_result
SELECT tables.table_name, 'merge',
       CASE WHEN getvariable('thelake_compaction_enabled') AND tables.table_exists
         THEN 'completed' ELSE 'skipped' END,
       coalesce(merge.files_processed, 0), coalesce(merge.files_created, 0)
FROM thelake_maintenance_tables AS tables
LEFT JOIN thelake_merge_results AS merge USING (table_name);

SELECT CASE WHEN (getvariable('thelake_lease_epoch') = 0 AND NOT EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = 'physical_scope_maintenance'
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND lease_until > now()
)) OR EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = getvariable('thelake_lease_job')
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND holder_id = getvariable('thelake_lease_holder')
    AND epoch = getvariable('thelake_lease_epoch')
    AND lease_until > now()
) THEN TRUE ELSE error('maintenance lease lost') END;
INSERT INTO __thelake_registry."{{registry_schema}}".compaction_watermark
  (scope_key, table_name, watermark, updated_at)
SELECT getvariable('thelake_maintenance_scope_key'), table_name,
       getvariable('thelake_maintenance_pass_started_at'), now()
FROM thelake_merge_results
WHERE getvariable('thelake_compaction_enabled')
ON CONFLICT (scope_key, table_name) DO UPDATE
SET watermark = excluded.watermark, updated_at = excluded.updated_at;
INSERT INTO __thelake_registry."{{registry_schema}}".maintenance_outcome
SELECT getvariable('thelake_maintenance_scope_key'), getvariable('thelake_maintenance_pass_started_at'),
       table_name, action, status, files_processed, files_created
FROM thelake_maintenance_result
ON CONFLICT (scope_key, table_name, action) DO UPDATE SET
  pass_started_at = excluded.pass_started_at,
  status = excluded.status,
  files_processed = excluded.files_processed,
  files_created = excluded.files_created;

SET VARIABLE thelake_cleanup_older_than = CASE
  WHEN getvariable('thelake_metadata_enabled') THEN
    current_timestamp - getvariable('thelake_reader_safety_grace_seconds') * INTERVAL '1 second'
  ELSE TIMESTAMPTZ '0001-01-01 00:00:00+00' END;
SELECT CASE WHEN (getvariable('thelake_lease_epoch') = 0 AND NOT EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = 'physical_scope_maintenance'
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND lease_until > now()
)) OR EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = getvariable('thelake_lease_job')
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND holder_id = getvariable('thelake_lease_holder')
    AND epoch = getvariable('thelake_lease_epoch')
    AND lease_until > now()
) THEN TRUE ELSE error('maintenance lease lost') END;
CREATE OR REPLACE TEMP TABLE thelake_expired_snapshots AS
SELECT * FROM ducklake_expire_snapshots(
  getvariable('thelake_catalog_alias'), older_than => getvariable('thelake_cleanup_older_than'));
INSERT INTO thelake_maintenance_result
SELECT '*', 'expire_snapshots',
       CASE WHEN getvariable('thelake_metadata_enabled') THEN 'completed' ELSE 'skipped' END,
       count(*), 0 FROM thelake_expired_snapshots;

SELECT CASE WHEN (getvariable('thelake_lease_epoch') = 0 AND NOT EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = 'physical_scope_maintenance'
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND lease_until > now()
)) OR EXISTS (
  SELECT 1 FROM __thelake_registry."{{registry_schema}}".thelake_job_lease
  WHERE job_name = getvariable('thelake_lease_job')
    AND scope_key = getvariable('thelake_maintenance_scope_key')
    AND holder_id = getvariable('thelake_lease_holder')
    AND epoch = getvariable('thelake_lease_epoch')
    AND lease_until > now()
) THEN TRUE ELSE error('maintenance lease lost') END;
CREATE OR REPLACE TEMP TABLE thelake_cleaned_scheduled_files AS
SELECT * FROM ducklake_cleanup_old_files(
  getvariable('thelake_catalog_alias'), older_than => getvariable('thelake_cleanup_older_than'));
INSERT INTO thelake_maintenance_result
SELECT '*', 'cleanup_scheduled_files',
       CASE WHEN getvariable('thelake_metadata_enabled') THEN 'completed' ELSE 'skipped' END,
       count(*), 0 FROM thelake_cleaned_scheduled_files;

INSERT INTO __thelake_registry."{{registry_schema}}".maintenance_outcome
SELECT getvariable('thelake_maintenance_scope_key'), getvariable('thelake_maintenance_pass_started_at'),
       table_name, action, status, files_processed, files_created
FROM thelake_maintenance_result
WHERE action IN ('expire_snapshots', 'cleanup_scheduled_files')
ON CONFLICT (scope_key, table_name, action) DO UPDATE SET
  pass_started_at = excluded.pass_started_at,
  status = excluded.status,
  files_processed = excluded.files_processed,
  files_created = excluded.files_created;

SELECT * FROM thelake_maintenance_result ORDER BY table_name, action;
