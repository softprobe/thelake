use anyhow::{anyhow, Result};
use duckdb::Connection;

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

/// Build the `CREATE TABLE IF NOT EXISTS` DDL for an OTLP table with
/// `PARTITIONED BY`, `SORTED BY`, and `WITH (...)` options inline.
pub(crate) fn otlp_create_table_ddl(
    conn: &Connection,
    base_ddl: &str,
    table_name: &str,
) -> Result<String> {
    if !is_otlp_table(table_name) {
        return Ok(base_ddl.to_string());
    }
    let profile = load_otlp_layout_profile(conn)?;
    let trimmed = base_ddl.trim_end().trim_end_matches(';');
    Ok(format!(
        "{trimmed} PARTITIONED BY ({}) SORTED BY ({}) WITH (\
            target_file_size = '{}',\
            parquet_row_group_size_bytes = '{}',\
            parquet_compression = '{}',\
            parquet_compression_level = {},\
            data_inlining_row_limit = {},\
            sort_on_insert = {},\
            per_thread_output = {},\
            hive_file_pattern = true\
        );",
        profile.partition_by,
        profile.sorted_by,
        super::util::size_literal(profile.target_file_size_bytes as usize),
        super::util::size_literal(profile.row_group_size_bytes as usize),
        profile.parquet_compression.replace('\'', "''"),
        profile.parquet_compression_level,
        profile.data_inlining_row_limit,
        profile.sort_on_insert,
        profile.per_thread_output,
    ))
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
        assert_eq!(profile.sorted_by, "session_id, timestamp, trace_id");
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

    #[test]
    fn create_table_ddl_supports_partition_sort_and_with_options() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let meta = tmp.path().join("meta.ducklake");
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).expect("create data dir");
        let cfg = duckdb::Config::default()
            .with("allow_unsigned_extensions", "true")
            .expect("duckdb config");
        let conn = Connection::open_in_memory_with_flags(cfg).expect("open DuckDB");
        let ext = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/ducklake-extension/ducklake.duckdb_extension");
        conn.execute_batch(&format!(
            "LOAD '{}'; ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}');",
            ext.display(),
            meta.display(),
            data.display()
        ))
        .expect("attach ducklake");
        let profile = load_otlp_layout_profile(&conn).expect("profile");
        let base_ddl = "CREATE TABLE IF NOT EXISTS lake.traces (\
            session_id VARCHAR NOT NULL,\
            trace_id VARCHAR NOT NULL,\
            timestamp TIMESTAMP_NS NOT NULL\
        );";
        let ddl = otlp_create_table_ddl(&conn, base_ddl, "traces").expect("build DDL");
        conn.execute_batch(&ddl).expect("CREATE TABLE with DDL options");
        assert!(
            crate::storage::schema::ducklake_partition::table_partition_sort_matches(
                &conn,
                "lake.traces",
                "traces",
                &profile.partition_by,
                &profile.sorted_by,
            )
            .expect("check partition/sort"),
            "CREATE TABLE DDL must persist partition and sort keys"
        );
    }
}
