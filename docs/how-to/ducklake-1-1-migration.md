# DuckLake 1.0 to 1.1 catalog migration

DuckLake 1.1 reads and writes existing DuckLake 1.0 PostgreSQL catalogs without schema changes. Opt-in catalog migration unlocks DuckLake 1.1 metadata features such as exact string statistics, `row_group_count` tracking on `ducklake_data_file`, and `BYTEA` `VARIANT` inlining.

## Prerequisites

- Upgrade `thelake` to the DuckDB 2.0 + DuckLake 1.1 runtime first.
- Verify the target PostgreSQL schema (`__ducklake_metadata_<catalog_alias>`) and backup storage have enough space for a schema dump.
- Pause concurrent catalog writers or run the migration on a cloned/snapshotted staging database first.

## 1. Back up the PostgreSQL metadata schema

Back up the DuckLake metadata schema before running `AUTOMATIC_MIGRATION true`:

```bash
pg_dump \
  --dbname="host=<pg-host> port=5432 dbname=<db> user=<user>" \
  --schema="__ducklake_metadata_<catalog_alias>" \
  --format=custom \
  --file="ducklake_metadata_1_0_backup.dump"
```

Record the pre-migration catalog version:

```sql
SELECT key, value
FROM "__ducklake_metadata_<catalog_alias>".ducklake_metadata
WHERE key IN ('version', 'created_by');
```

## 2. Run opt-in migration

Attach the catalog once with `AUTOMATIC_MIGRATION true` (or invoke `migrate_ducklake_catalog_from_config` from Rust):

```sql
ATTACH 'ducklake:postgres:host=<pg-host> port=5432 dbname=<db> user=<user> password=<password>'
  AS <catalog_alias> (
    DATA_PATH '<warehouse-uri>',
    METADATA_SCHEMA '<metadata-schema>',
    META_SCHEMA '<metadata-schema>',
    DATA_INLINING_ROW_LIMIT 500,
    AUTOMATIC_MIGRATION true
  );
```

## 3. Verify the migrated catalog

Confirm the metadata version updated to `1.1` and new 1.1 columns exist:

```sql
SELECT key, value
FROM "__ducklake_metadata_<catalog_alias>".ducklake_metadata
WHERE key = 'version';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = '__ducklake_metadata_<catalog_alias>'
  AND table_name = 'ducklake_data_file'
  AND column_name = 'row_group_count';
```

Verify existing reads and post-migration writes succeed across `traces`, `logs`, `scores`, and `score_configs`, and confirm newly written Parquet files populate `ducklake_data_file.row_group_count`:

```sql
SELECT data_file_id, record_count, row_group_count
FROM "__ducklake_metadata_<catalog_alias>".ducklake_data_file
ORDER BY data_file_id DESC
LIMIT 10;
```

## 4. Rollback

If migration validation fails before production writes proceed on `1.1`, restore the pre-migration schema from the backup:

```bash
psql "host=<pg-host> port=5432 dbname=<db> user=<user>" \
  -c 'DROP SCHEMA "__ducklake_metadata_<catalog_alias>" CASCADE;'

pg_restore \
  --dbname="host=<pg-host> port=5432 dbname=<db> user=<user>" \
  ducklake_metadata_1_0_backup.dump
```
