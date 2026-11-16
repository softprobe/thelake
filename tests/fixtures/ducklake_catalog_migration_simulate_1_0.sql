UPDATE "{{SCHEMA}}".ducklake_metadata SET value = '1.0' WHERE key = 'version';
ALTER TABLE "{{SCHEMA}}".ducklake_data_file DROP COLUMN IF EXISTS row_group_count;
ALTER TABLE "{{SCHEMA}}".ducklake_delete_file DROP COLUMN IF EXISTS row_group_count;
ALTER TABLE "{{SCHEMA}}".ducklake_file_column_stats DROP COLUMN IF EXISTS min_is_exact;
ALTER TABLE "{{SCHEMA}}".ducklake_file_column_stats DROP COLUMN IF EXISTS max_is_exact;
ALTER TABLE "{{SCHEMA}}".ducklake_table_column_stats DROP COLUMN IF EXISTS min_is_exact;
ALTER TABLE "{{SCHEMA}}".ducklake_table_column_stats DROP COLUMN IF EXISTS max_is_exact;
ALTER TABLE "{{SCHEMA}}".ducklake_schema DROP COLUMN IF EXISTS parent_schema_id;
DROP TABLE IF EXISTS "{{SCHEMA}}".ducklake_view_column_tag;
