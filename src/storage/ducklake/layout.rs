use anyhow::{anyhow, Result};
use duckdb::Connection;

use super::{ducklake_set_option_scope_for_qualified, PhysicalScope};
use crate::sql::schema::{is_otlp_table, OTLP_LAYOUT_SQL};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OtlpLayoutProfile {
    pub partition_by: String,
    pub sorted_by: String,
    pub row_group_size_bytes: u64,
    pub target_file_size_bytes: u64,
    pub parquet_compression: String,
    pub parquet_compression_level: u64,
    pub data_inlining_row_limit: u64,
    pub sort_on_insert: bool,
    pub per_thread_output: bool,
    pub preserve_insertion_order: bool,
}

pub(crate) fn load_otlp_layout_profile(conn: &Connection) -> Result<OtlpLayoutProfile> {
    conn.execute_batch(OTLP_LAYOUT_SQL)
        .map_err(|error| anyhow!("failed to load canonical OTLP layout profile: {error}"))?;
    conn.query_row(
        "SELECT getvariable('thelake_otlp_partition_by'), \
                getvariable('thelake_otlp_sorted_by'), \
                getvariable('thelake_otlp_row_group_size_bytes'), \
                getvariable('thelake_otlp_target_file_size_bytes'), \
                getvariable('thelake_otlp_parquet_compression'), \
                getvariable('thelake_otlp_parquet_compression_level'), \
                getvariable('thelake_otlp_data_inlining_row_limit'), \
                getvariable('thelake_otlp_sort_on_insert'), \
                getvariable('thelake_otlp_per_thread_output'), \
                getvariable('thelake_otlp_preserve_insertion_order')",
        [],
        |row| {
            Ok(OtlpLayoutProfile {
                partition_by: row.get(0)?,
                sorted_by: row.get(1)?,
                row_group_size_bytes: row.get(2)?,
                target_file_size_bytes: row.get(3)?,
                parquet_compression: row.get(4)?,
                parquet_compression_level: row.get(5)?,
                data_inlining_row_limit: row.get(6)?,
                sort_on_insert: row.get(7)?,
                per_thread_output: row.get(8)?,
                preserve_insertion_order: row.get(9)?,
            })
        },
    )
    .map_err(|error| anyhow!("failed to read canonical OTLP layout profile: {error}"))
}

/// Apply the shared physical settings to a table so inserts, flushes and
/// compaction all use the same persisted DuckLake options.
pub(crate) fn apply_otlp_layout_profile(
    conn: &Connection,
    scope: &PhysicalScope,
    table: &str,
) -> Result<()> {
    if !is_otlp_table(table) {
        return Ok(());
    }
    let profile = load_otlp_layout_profile(conn)?;
    apply_otlp_writer_session_with_profile(conn, &profile)?;
    let qualified = super::ducklake_qualified_table_name(scope, table);
    let option_scope = ducklake_set_option_scope_for_qualified(&qualified);
    let alias = scope.attach_alias();
    let statements = [
        format!(
            "CALL {alias}.set_option('target_file_size', '{}', {option_scope});",
            super::util::size_literal(profile.target_file_size_bytes as usize)
        ),
        format!(
            "CALL {alias}.set_option('parquet_row_group_size_bytes', '{}', {option_scope});",
            super::util::size_literal(profile.row_group_size_bytes as usize)
        ),
        format!(
            "CALL {alias}.set_option('parquet_compression', '{}', {option_scope});",
            profile.parquet_compression.replace('\'', "''")
        ),
        format!(
            "CALL {alias}.set_option('parquet_compression_level', '{}', {option_scope});",
            profile.parquet_compression_level
        ),
        format!(
            "CALL {alias}.set_option('data_inlining_row_limit', {}, {option_scope});",
            profile.data_inlining_row_limit
        ),
        format!(
            "CALL {alias}.set_option('sort_on_insert', {}, {option_scope});",
            profile.sort_on_insert
        ),
        format!(
            "CALL {alias}.set_option('per_thread_output', {}, {option_scope});",
            profile.per_thread_output
        ),
        format!("CALL {alias}.set_option('hive_file_pattern', true, {option_scope});"),
    ];
    conn.execute_batch(&statements.join("\n"))
        .map_err(|error| anyhow!("failed to apply canonical OTLP layout to {qualified}: {error}"))
}

/// Apply the DuckDB session prerequisite for byte-sized row groups.
/// This must run on the same connection as each insert that can write files.
pub(crate) fn apply_otlp_writer_session(conn: &Connection, table: &str) -> Result<()> {
    if !is_otlp_table(table) {
        return Ok(());
    }
    let profile = load_otlp_layout_profile(conn)?;
    apply_otlp_writer_session_with_profile(conn, &profile)
}

fn apply_otlp_writer_session_with_profile(
    conn: &Connection,
    profile: &OtlpLayoutProfile,
) -> Result<()> {
    conn.execute_batch(&format!(
        "SET preserve_insertion_order = {};",
        profile.preserve_insertion_order
    ))
    .map_err(|error| anyhow!("failed to set Parquet writer session profile: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_values_are_loaded_from_one_sql_source() {
        let conn = Connection::open_in_memory().expect("DuckDB connection");
        let profile = load_otlp_layout_profile(&conn).expect("profile");
        assert_eq!(
            profile.partition_by,
            "year(timestamp), month(timestamp), day(timestamp)"
        );
        assert_eq!(profile.sorted_by, "session_id, trace_id, timestamp");
        assert_eq!(profile.row_group_size_bytes, 8 * 1024 * 1024);
        assert_eq!(profile.target_file_size_bytes, 128 * 1024 * 1024);
        assert_eq!(profile.parquet_compression, "zstd");
        assert_eq!(profile.parquet_compression_level, 3);
        assert_eq!(profile.data_inlining_row_limit, 500);
        assert!(profile.sort_on_insert);
        assert!(!profile.per_thread_output);
        assert!(!profile.preserve_insertion_order);
    }

    #[test]
    fn only_telemetry_fact_tables_receive_the_parquet_profile() {
        for table in ["traces", "logs", "scores"] {
            assert!(is_otlp_table(table));
        }
        assert!(!is_otlp_table("score_configs"));
    }
}
