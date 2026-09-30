//! SQL-owned DuckLake compaction, watermark, and cleanup pass.

use crate::sql::literal::sql_string_literal;

const MAINTENANCE_SQL: &str = include_str!("maintenance.sql");
const NEWER_THAN_CAPABILITY_SQL: &str = "SELECT count(*) > 0 FROM duckdb_functions() \
     WHERE function_name = 'ducklake_merge_adjacent_files' \
       AND list_contains(parameters, 'newer_than')";

pub(crate) fn newer_than_capability_sql() -> &'static str {
    NEWER_THAN_CAPABILITY_SQL
}

pub(crate) fn scope_config_upsert_sql(registry_schema: &str) -> String {
    format!(
        "INSERT INTO __thelake_registry.\"{}\".maintenance_scope_config \
         (scope_key, catalog_alias, metadata_schema, compaction_enabled, metadata_enabled, \
          reader_safety_grace_seconds, lease_job, lease_holder, lease_epoch) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (scope_key, lease_epoch) DO UPDATE SET \
           catalog_alias = excluded.catalog_alias, metadata_schema = excluded.metadata_schema, \
           compaction_enabled = excluded.compaction_enabled, metadata_enabled = excluded.metadata_enabled, \
           reader_safety_grace_seconds = excluded.reader_safety_grace_seconds, \
           lease_job = excluded.lease_job, lease_holder = excluded.lease_holder, lease_epoch = excluded.lease_epoch",
        registry_schema.replace('"', "\"\"")
    )
}

pub struct MaintenanceSqlParams<'a> {
    pub registry_schema: &'a str,
    pub scope_key: &'a str,
    pub lease_epoch: i64,
}

pub fn live_file_sizes_sql(catalog_alias: &str, table: &str) -> String {
    let metadata_schema = format!("__ducklake_metadata_{catalog_alias}");
    format!(
        "SELECT df.file_size_bytes::BIGINT AS file_size_bytes \
         FROM {metadata_schema}.ducklake_data_file df \
         JOIN {metadata_schema}.ducklake_table t ON df.table_id = t.table_id \
         WHERE t.table_name = {} AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL",
        sql_string_literal(table)
    )
}

pub fn render_maintenance_sql(params: &MaintenanceSqlParams<'_>) -> String {
    let mut sql = MAINTENANCE_SQL.to_string();
    sql = sql.replace("{{otlp_layout_sql}}", crate::sql::schema::OTLP_LAYOUT_SQL);
    sql = sql.replace(
        "{{registry_schema}}",
        &params.registry_schema.replace('"', "\"\""),
    );
    sql = sql.replace("{{scope_key}}", &escape_sql_content(params.scope_key));
    sql = sql.replace("{{lease_epoch}}", &params.lease_epoch.to_string());
    sql
}

fn escape_sql_content(input: &str) -> String {
    input.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> MaintenanceSqlParams<'static> {
        MaintenanceSqlParams {
            registry_schema: "softprobe",
            scope_key: "scope-key",
            lease_epoch: 3,
        }
    }

    #[test]
    fn maintenance_sql_uses_one_watermarked_merge_per_table_and_captures_results() {
        let sql = render_maintenance_sql(&params());
        assert!(sql.contains(
            "SET preserve_insertion_order = getvariable('thelake_otlp_preserve_insertion_order')"
        ));
        assert!(sql.contains("SET VARIABLE thelake_otlp_row_group_size_bytes = 8388608"));
        assert_eq!(sql.matches("ducklake_merge_adjacent_files(").count(), 1);
        assert_eq!(sql.matches("newer_than =>").count(), 1);
        assert!(sql.contains("VALUES ('traces'), ('logs'), ('scores')"));
        assert!(sql.contains("LEFT JOIN __thelake_registry.\"softprobe\".compaction_watermark"));
        assert!(sql.contains("CREATE OR REPLACE TEMP TABLE thelake_merge_results"));
        assert!(sql.contains("SET VARIABLE thelake_merge_sql = ("));
        assert!(sql.contains("SELECT * FROM query(getvariable('thelake_merge_sql'))"));
        assert!(sql.contains("watermark.watermark, TIMESTAMPTZ '0001-01-01"));
        assert!(sql.contains("TIMESTAMP ''{}'' AT TIME ZONE ''UTC''"));
        assert!(sql.contains("WHERE getvariable('thelake_compaction_enabled')"));
        assert!(sql.contains(
            "WHERE getvariable('thelake_compaction_enabled')\n    AND tables.table_exists"
        ));
        assert!(sql.contains("lake.table_name IS NOT NULL AS table_exists"));
        assert!(sql.contains("TIMESTAMPTZ '0001-01-01"));
        assert!(sql.contains("files_processed"));
        assert!(sql.contains("files_created"));
        assert!(sql.contains("maintenance_scope_config"));
        assert!(sql.contains("lease_epoch = 3"));
        assert!(sql.contains("lake.database_name = getvariable('thelake_catalog_alias')"));
        assert!(sql.contains(
            "FROM thelake_merge_results\nWHERE getvariable('thelake_compaction_enabled')"
        ));
        assert!(sql.contains("reader_safety_grace_seconds"));
        assert!(sql.contains("getvariable('thelake_lease_holder')"));
        assert!(sql.contains("The output layout (day partition"));
        assert!(!sql.contains("{{catalog_alias"));
        assert!(!sql.contains("{{metadata_schema"));
        assert!(!sql.contains("{{reader_safety_grace_seconds"));
        assert!(!sql.contains("max_file_size"));
        assert!(!sql.contains("ducklake_delete_orphaned_files"));
        assert!(
            sql.find("ducklake_expire_snapshots").unwrap()
                < sql.find("ducklake_cleanup_old_files").unwrap()
        );
        assert!(
            sql.find("CREATE OR REPLACE TEMP TABLE thelake_merge_results")
                .unwrap()
                < sql
                    .find("INSERT INTO __thelake_registry.\"softprobe\".compaction_watermark")
                    .unwrap()
        );
        assert!(
            sql.find("INSERT INTO __thelake_registry.\"softprobe\".compaction_watermark")
                .unwrap()
                < sql.find("ducklake_expire_snapshots").unwrap()
        );
        assert!(
            sql.contains("thelake_expired_snapshots AS\nSELECT * FROM ducklake_expire_snapshots")
        );
        assert!(sql.contains(
            "thelake_cleaned_scheduled_files AS\nSELECT * FROM ducklake_cleanup_old_files"
        ));
    }

    #[test]
    fn sql_settings_control_compaction_and_cleanup_without_rust_branches() {
        let sql = render_maintenance_sql(&params());
        assert_eq!(sql.matches("ducklake_merge_adjacent_files(").count(), 1);
        assert!(sql.contains(
            "CASE WHEN getvariable('thelake_compaction_enabled') AND tables.table_exists"
        ));
        assert!(sql.contains("WHERE getvariable('thelake_compaction_enabled')"));
        assert!(sql.contains(
            "CASE WHEN getvariable('thelake_metadata_enabled') THEN 'completed' ELSE 'skipped' END"
        ));
        assert!(sql.contains("ducklake_expire_snapshots"));
        assert!(sql.contains("ducklake_cleanup_old_files"));
    }

    #[test]
    fn lease_checks_are_sql_owned_and_context_is_catalog_backed() {
        let sql = render_maintenance_sql(&params());
        assert!(sql.contains("thelake_job_lease"));
        assert!(sql.contains("epoch = getvariable('thelake_lease_epoch')"));
        assert!(sql.contains("getvariable('thelake_lease_epoch') = 0 AND NOT EXISTS"));
        assert!(sql.contains("maintenance_scope_config"));
        assert!(!sql.contains("{{lease_"));
    }

    #[test]
    fn sql_inputs_are_quoted() {
        let mut p = params();
        p.scope_key = "scope'key";
        let sql = render_maintenance_sql(&p);
        assert!(sql.contains("'scope''key'"));
    }

    #[test]
    fn scope_configuration_upsert_is_parameterized_and_schema_quoted() {
        let sql = scope_config_upsert_sql("registry\"schema");
        assert!(sql.contains("__thelake_registry.\"registry\"\"schema\".maintenance_scope_config"));
        assert!(sql.contains("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"));
        assert!(sql.contains("ON CONFLICT (scope_key, lease_epoch) DO UPDATE"));
    }
}
