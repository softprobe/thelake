use crate::config::Config;
use anyhow::{bail, Context, Result};
use duckdb::Connection;

const EVENT_TYPE: &str =
    r#"STRUCT("name" VARCHAR, "timestamp" TIMESTAMP_NS, attributes MAP(VARCHAR, VARCHAR))[]"#;

pub fn migrate_trace_events(config: &Config) -> Result<()> {
    let conn =
        super::open_attached_from_config(&config.ducklake, config.ducklake.data_inlining_row_limit);
    conn.execute_batch(
        "SET memory_limit='8GB'; SET threads=4; SET preserve_insertion_order=false;",
    )?;
    migrate(
        &conn,
        &config.ducklake.catalog_alias,
        &config.ducklake.metadata_schema,
        &config.ducklake.metadata_path,
        &config.ducklake.data_path,
        config.maintenance.target_file_size_bytes,
    )?;
    Ok(())
}

fn migrate(
    conn: &Connection,
    catalog: &str,
    schema: &str,
    metadata_dsn: &str,
    data_path: &str,
    target_file_size_bytes: usize,
) -> Result<()> {
    let catalog = quote_ident(catalog)?;
    let schema_ident = quote_ident(schema)?;
    conn.execute_batch(&format!(
        "ATTACH '{}' AS raw (TYPE POSTGRES);",
        escape_literal(metadata_dsn)
    ))?;

    let traces = format!("{catalog}.{schema_ident}.traces");
    if let Some(backup_name) = restore_interrupted_swap(conn, &catalog_name(&catalog), schema)? {
        bail!("restored traces from {backup_name} after an interrupted table swap; rerun the migration")
    }
    let event_type: String = conn
        .query_row(
            &format!(
                "SELECT data_type FROM duckdb_columns() WHERE database_name = '{}' AND schema_name = '{}' AND table_name = 'traces' AND column_name = 'events'",
                escape_literal(&catalog_name(catalog.as_str())),
                escape_literal(schema),
            ),
            [],
            |row| row.get(0),
        )
        .context("the configured DuckLake traces.events column does not exist")?;
    if event_type.eq_ignore_ascii_case("JSON") {
        println!("traces.events already uses JSON; any retained legacy table is left for post-validation cleanup");
        return Ok(());
    }
    if !event_type.to_ascii_uppercase().starts_with("STRUCT(") || !event_type.ends_with("[]") {
        bail!("traces.events has unsupported type {event_type}; expected the legacy LIST<STRUCT> type");
    }

    let table_id: i64 = conn.query_row(
        &format!(
            "SELECT table_id FROM raw.{schema_ident}.ducklake_table WHERE table_name='traces' AND end_snapshot IS NULL ORDER BY begin_snapshot DESC LIMIT 1"
        ),
        [],
        |row| row.get(0),
    ).context("active traces table is missing from the DuckLake catalog")?;
    let inline_tables = query_strings(
        conn,
        &format!(
            "SELECT i.table_name FROM raw.{schema_ident}.ducklake_inlined_data_tables i JOIN raw.information_schema.columns c ON c.table_schema = '{}' AND c.table_name = i.table_name AND c.column_name = 'events' WHERE i.table_id = {table_id} ORDER BY i.schema_version",
            escape_literal(schema),
        ),
    )?;
    if let Some(name) = inline_tables.iter().find(|name| !identifier_is_safe(name)) {
        bail!("unsafe inline trace table name in DuckLake metadata: {name}");
    }
    let parquet_paths = query_strings(
        conn,
        &format!(
            "SELECT path FROM raw.{schema_ident}.ducklake_data_file WHERE table_id = {table_id} AND end_snapshot IS NULL AND lower(file_format) = 'parquet' ORDER BY file_order"
        ),
    )?;
    let table_path: String = conn.query_row(
        &format!("SELECT path FROM raw.{schema_ident}.ducklake_table WHERE table_id = {table_id}"),
        [],
        |row| row.get(0),
    )?;
    let parquet_paths = parquet_paths
        .into_iter()
        .map(|path| data_file_path(data_path, schema, &table_path, &path))
        .collect::<Vec<_>>();

    let inline_query = inline_union(schema, &inline_tables)?;
    if !inline_tables.is_empty() {
        let invalid_casts: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM ({inline_query}) source WHERE events IS NOT NULL AND TRY_CAST(events AS {EVENT_TYPE}) IS NULL"
            ),
            [],
            |row| row.get(0),
        )?;
        if invalid_casts != 0 {
            bail!("cannot rebuild traces.events: {invalid_casts} active inline payloads cannot be read as the legacy event type");
        }
    }

    conn.execute_batch(&format!(
        "CREATE TEMP TABLE trace_event_base AS SELECT rowid AS _row_id, CAST(session_id AS VARCHAR) AS _session_id, CAST(trace_id AS VARCHAR) AS _trace_id, CAST(span_id AS VARCHAR) AS _span_id, * REPLACE (NULL::JSON AS events) FROM {traces};"
    )).context("failed to read trace rows without materializing the legacy events column")?;

    let mut event_queries = Vec::new();
    if !inline_tables.is_empty() {
        event_queries.push(format!(
            "SELECT row_id AS _row_id, CAST(session_id AS VARCHAR) AS _session_id, CAST(trace_id AS VARCHAR) AS _trace_id, CAST(span_id AS VARCHAR) AS _span_id, {} AS events FROM ({inline_query}) source WHERE events IS NOT NULL",
            events_json(&format!("TRY_CAST(events AS {EVENT_TYPE})"))
        ));
    }
    if !parquet_paths.is_empty() {
        let paths = parquet_paths
            .iter()
            .map(|path| format!("'{}'", escape_literal(path)))
            .collect::<Vec<_>>()
            .join(", ");
        let file_location =
            data_file_reference_expr("d.path", &data_file_prefix(data_path, schema, &table_path));
        event_queries.push(format!(
            "SELECT d.row_id_start + p.file_row_number AS _row_id, CAST(p.session_id AS VARCHAR) AS _session_id, CAST(p.trace_id AS VARCHAR) AS _trace_id, CAST(p.span_id AS VARCHAR) AS _span_id, {} AS events FROM read_parquet([{paths}], file_row_number=true, filename=true) p JOIN raw.{schema_ident}.ducklake_data_file d ON p.filename = {file_location} WHERE d.table_id = {table_id} AND d.end_snapshot IS NULL AND p.events IS NOT NULL",
            events_json("p.events"),
        ));
    }
    if event_queries.is_empty() {
        bail!("DuckLake has no active inline or Parquet trace event sources");
    }
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE trace_event_source AS {};",
        event_queries.join(" UNION ALL "),
    ))
    .context("failed to stage existing inline and Parquet trace events")?;
    let source_stats = verify_event_source_identity(conn)?;
    conn.execute_batch(
        "CREATE TEMP TABLE trace_events_json AS SELECT s._row_id, s._session_id, s._trace_id, s._span_id, s.events FROM trace_event_source s JOIN trace_event_base b USING (_row_id);",
    ).context("failed to stage active trace event payloads")?;

    let new_name = format!("traces_json_rebuild_{table_id}");
    let old_name = format!("traces_legacy_{table_id}");
    let new_table = format!("{catalog}.{schema_ident}.{}", quote_ident(&new_name)?);
    if table_exists(conn, &catalog_name(&catalog), schema, &old_name)? {
        bail!("found retained table {old_name}; inspect the completed or interrupted trace event migration before retrying");
    }
    if table_exists(conn, &catalog_name(&catalog), schema, &new_name)? {
        conn.execute_batch(&format!("DROP TABLE {new_table};"))
            .context(
                "failed to remove an incomplete replacement table while original traces is intact",
            )?;
    }
    conn.execute_batch(&format!(
        "CREATE TABLE {new_table} AS SELECT b.* EXCLUDE (_row_id) FROM trace_event_base b WHERE false;"
    )).context("failed to create the replacement traces schema")?;
    conn.execute_batch(&format!(
        "ALTER TABLE {new_table} SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));\
         ALTER TABLE {new_table} SET SORTED BY (session_id, trace_id, timestamp);\
         CALL {}.set_option('target_file_size', '{}', schema => '{}', table_name => '{}');",
        catalog_name(&catalog),
        size_literal(target_file_size_bytes),
        escape_literal(schema),
        new_name,
    )).context("failed to apply the global timestamp partition and session sort rules")?;
    conn.execute_batch(&format!(
        "INSERT INTO {new_table} SELECT b.* EXCLUDE (_row_id) REPLACE (e.events AS events) FROM trace_event_base b LEFT JOIN trace_events_json e USING (_row_id) ORDER BY session_id, trace_id, timestamp;"
    )).context("failed to write the replacement traces data")?;
    let expected_rows: i64 =
        conn.query_row("SELECT count(*) FROM trace_event_base", [], |row| {
            row.get(0)
        })?;
    let rebuilt_rows: i64 =
        conn.query_row(&format!("SELECT count(*) FROM {new_table}"), [], |row| {
            row.get(0)
        })?;
    if rebuilt_rows != expected_rows {
        bail!("rebuilt traces row count changed from {expected_rows} to {rebuilt_rows}");
    }
    verify_rebuilt(conn, &new_table, expected_rows)?;
    verify_event_payloads(conn, &new_table, source_stats.matched)?;

    conn.execute_batch(&format!(
        "ALTER TABLE {traces} RENAME TO {old_name}; ALTER TABLE {new_table} RENAME TO traces;"
    ))
    .context("table rename interrupted; retained table names identify the recovery state")?;
    verify_rebuilt(conn, &traces, expected_rows)?;
    verify_event_payloads(conn, &traces, source_stats.matched)?;
    println!(
        "rebuilt traces.events as JSON; preserved {} active event payloads; retained {} stale source rows with no active trace and backup table {old_name}",
        source_stats.matched, source_stats.orphans
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct EventSourceStats {
    matched: i64,
    orphans: i64,
}

fn verify_event_source_identity(conn: &Connection) -> Result<EventSourceStats> {
    let stats: (i64, i64, i64, i64, i64) = conn.query_row(
        "SELECT count(*), count(b._row_id), count(*) FILTER (WHERE b._row_id IS NOT NULL AND (s._session_id IS DISTINCT FROM b._session_id OR s._trace_id IS DISTINCT FROM b._trace_id OR s._span_id IS DISTINCT FROM b._span_id)), count(*) FILTER (WHERE b._row_id IS NULL AND EXISTS (SELECT 1 FROM trace_event_base other WHERE other._session_id = s._session_id AND other._trace_id = s._trace_id AND other._span_id = s._span_id)), count(*) - count(DISTINCT s._row_id) FROM trace_event_source s LEFT JOIN trace_event_base b USING (_row_id)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )?;
    let (source_rows, matched, identity_mismatches, misplaced_rows, duplicate_rows) = stats;
    if identity_mismatches != 0 {
        bail!("cannot rebuild traces.events: {identity_mismatches} source row ids identify a different session/trace/span in the active trace table");
    }
    if misplaced_rows != 0 {
        bail!("cannot rebuild traces.events: {misplaced_rows} source payloads match an active session/trace/span under a different row id");
    }
    if duplicate_rows != 0 {
        bail!("cannot rebuild traces.events: {duplicate_rows} event source rows duplicate an active row id");
    }
    let orphans = source_rows - matched;
    Ok(EventSourceStats { matched, orphans })
}

fn verify_event_payloads(conn: &Connection, table: &str, expected: i64) -> Result<()> {
    let (actual, mismatches): (i64, i64) = conn.query_row(
        &format!(
            "SELECT count(*), count(*) FILTER (WHERE source.events::VARCHAR IS DISTINCT FROM target.events::VARCHAR) FROM trace_events_json source JOIN {table} target ON CAST(target.session_id AS VARCHAR) = source._session_id AND CAST(target.trace_id AS VARCHAR) = source._trace_id AND CAST(target.span_id AS VARCHAR) = source._span_id"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if actual != expected {
        bail!("rebuilt traces table matched {actual} event payloads; expected {expected}");
    }
    if mismatches != 0 {
        bail!("rebuilt traces table changed {mismatches} event payloads");
    }
    Ok(())
}

fn table_exists(conn: &Connection, catalog: &str, schema: &str, table: &str) -> Result<bool> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? AND table_name = ?)",
        [catalog, schema, table],
        |row| row.get(0),
    )?;
    Ok(exists)
}

fn restore_interrupted_swap(
    conn: &Connection,
    catalog: &str,
    schema: &str,
) -> Result<Option<String>> {
    if table_exists(conn, catalog, schema, "traces")? {
        return Ok(None);
    }
    let backups = query_strings(
        conn,
        &format!(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = '{}' AND schema_name = '{}' AND starts_with(table_name, 'traces_legacy_') ORDER BY table_name",
            escape_literal(catalog),
            escape_literal(schema),
        ),
    )?;
    if backups.len() != 1 {
        bail!("configured DuckLake has no active traces table and {} retained legacy tables; inspect the cutover state before proceeding", backups.len());
    }
    let backup_name = &backups[0];
    let suffix = backup_name.trim_start_matches("traces_legacy_");
    let rebuild_name = format!("traces_json_rebuild_{suffix}");
    let catalog_ident = quote_ident(catalog)?;
    let schema_ident = quote_ident(schema)?;
    if table_exists(conn, catalog, schema, &rebuild_name)? {
        conn.execute_batch(&format!(
            "DROP TABLE {catalog_ident}.{schema_ident}.{};",
            quote_ident(&rebuild_name)?
        ))
        .context("failed to remove the incomplete replacement before restoring traces")?;
    }
    conn.execute_batch(&format!(
        "ALTER TABLE {catalog_ident}.{schema_ident}.{} RENAME TO traces;",
        quote_ident(backup_name)?
    ))
    .context("failed to restore the retained traces table after an interrupted swap")?;
    Ok(Some(backup_name.clone()))
}

fn verify_rebuilt(conn: &Connection, table: &str, expected_rows: i64) -> Result<()> {
    let count: i64 = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get(0)
    })?;
    let event_type: String = conn.query_row(
        &format!("SELECT typeof(events) FROM {table} LIMIT 1"),
        [],
        |row| row.get(0),
    )?;
    if !event_type.eq_ignore_ascii_case("JSON") {
        bail!("rebuilt table has events type {event_type}, expected JSON");
    }
    let invalid: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM {table} WHERE events IS NOT NULL AND NOT json_valid(events)"
        ),
        [],
        |row| row.get(0),
    )?;
    if invalid != 0 {
        bail!("rebuilt table contains {invalid} invalid JSON event payloads");
    }
    let partition_key_type: String = conn.query_row(
        &format!("SELECT typeof(timestamp) FROM {table} LIMIT 1"),
        [],
        |row| row.get(0),
    )?;
    if !partition_key_type.eq_ignore_ascii_case("TIMESTAMP_NS") {
        bail!("rebuilt table has timestamp partition key type {partition_key_type}, expected TIMESTAMP_NS");
    }
    if count != expected_rows {
        bail!("rebuilt traces row count changed from {expected_rows} to {count}");
    }
    Ok(())
}

fn inline_union(schema: &str, tables: &[String]) -> Result<String> {
    if tables.is_empty() {
        return Ok(String::new());
    }
    let schema = quote_ident(schema)?;
    let selects = tables
        .iter()
        .map(|table| {
            Ok(format!(
                "SELECT row_id, session_id, trace_id, span_id, events FROM raw.{schema}.{}",
                quote_ident(table)?
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(selects.join(" UNION ALL "))
}

fn events_json(source: &str) -> String {
    format!(
        "CAST(to_json(list_transform({source}, lambda e: struct_pack(name := e.name, \"timestamp\" := strftime(e.timestamp, '%Y-%m-%dT%H:%M:%S') || '.' || lpad(CAST(((epoch_ns(e.timestamp) % 1000000000 + 1000000000) % 1000000000) AS VARCHAR), 9, '0') || 'Z', attributes := e.attributes))) AS JSON)"
    )
}

fn query_strings(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare(sql)?;
    let values = statement.query_map([], |row| row.get::<_, String>(0))?;
    values
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn data_file_path(root: &str, schema: &str, table_path: &str, path: &str) -> String {
    if path.contains("://") || path.starts_with('/') {
        return path.to_string();
    }
    format!(
        "{}/{}/{}/{}",
        root.trim_end_matches('/'),
        schema,
        table_path.trim_matches('/'),
        path.trim_start_matches('/')
    )
}

fn data_file_prefix(root: &str, schema: &str, table_path: &str) -> String {
    format!(
        "{}/{}/{}/",
        root.trim_end_matches('/'),
        schema,
        table_path.trim_matches('/')
    )
}

fn data_file_reference_expr(path: &str, relative_prefix: &str) -> String {
    format!(
        "CASE WHEN contains({path}, '://') OR starts_with({path}, '/') THEN {path} ELSE {} || {path} END",
        sql_literal(relative_prefix)
    )
}

fn quote_ident(value: &str) -> Result<String> {
    if !identifier_is_safe(value) {
        bail!("unsafe SQL identifier in DuckLake config or catalog: {value}");
    }
    Ok(format!("\"{value}\""))
}

fn identifier_is_safe(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", escape_literal(value))
}

fn catalog_name(quoted: &str) -> String {
    quoted.trim_matches('"').to_string()
}

fn size_literal(bytes: usize) -> String {
    for (unit, suffix) in [
        (1usize << 30, "GB"),
        (1usize << 20, "MB"),
        (1usize << 10, "KB"),
    ] {
        if bytes >= unit && bytes.is_multiple_of(unit) {
            return format!("{}{suffix}", bytes / unit);
        }
    }
    format!("{bytes}B")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_connection(source_rows: &str) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TEMP TABLE trace_event_base (_row_id BIGINT, _session_id VARCHAR, _trace_id VARCHAR, _span_id VARCHAR); CREATE TEMP TABLE trace_event_source (_row_id BIGINT, _session_id VARCHAR, _trace_id VARCHAR, _span_id VARCHAR, events JSON);",
        )
        .unwrap();
        conn.execute_batch(&format!(
            "INSERT INTO trace_event_base VALUES (1, 's1', 't1', 'p1'), (2, 's2', 't2', 'p2'); INSERT INTO trace_event_source VALUES {source_rows};"
        ))
        .unwrap();
        conn
    }

    #[test]
    fn restores_legacy_table_after_interrupted_swap() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE traces_legacy_17 (id INTEGER); INSERT INTO traces_legacy_17 VALUES (1); CREATE TABLE traces_json_rebuild_17 (id INTEGER);",
        )
        .unwrap();
        assert_eq!(
            restore_interrupted_swap(&conn, "memory", "main").unwrap(),
            Some("traces_legacy_17".to_string())
        );
        assert!(table_exists(&conn, "memory", "main", "traces").unwrap());
        assert!(!table_exists(&conn, "memory", "main", "traces_json_rebuild_17").unwrap());
        let id: i32 = conn
            .query_row("SELECT id FROM traces", [], |row| row.get(0))
            .unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn validates_inline_and_parquet_event_payload_sources_by_identity() {
        let conn = source_connection(
            "(1, 's1', 't1', 'p1', '[{\"name\":\"inline\"}]'), (2, 's2', 't2', 'p2', '[{\"name\":\"parquet\"}]')",
        );
        assert_eq!(
            verify_event_source_identity(&conn).unwrap(),
            EventSourceStats {
                matched: 2,
                orphans: 0,
            }
        );
        conn.execute_batch(
            "CREATE TEMP TABLE trace_events_json AS SELECT * FROM trace_event_source; CREATE TEMP TABLE rebuilt AS SELECT _session_id AS session_id, _trace_id AS trace_id, _span_id AS span_id, events FROM trace_events_json;",
        )
        .unwrap();
        verify_event_payloads(&conn, "rebuilt", 2).unwrap();
    }

    #[test]
    fn classifies_only_unmatched_inactive_sources_as_orphans() {
        let conn = source_connection(
            "(1, 's1', 't1', 'p1', '[{\"name\":\"active\"}]'), (3, 'stale', 'old', 'p3', '[{\"name\":\"stale\"}]')",
        );
        assert_eq!(
            verify_event_source_identity(&conn).unwrap(),
            EventSourceStats {
                matched: 1,
                orphans: 1,
            }
        );
    }

    #[test]
    fn rejects_row_id_pointing_at_a_different_trace() {
        let conn = source_connection("(1, 's2', 't2', 'p2', '[{\"name\":\"wrong\"}]')");
        assert!(verify_event_source_identity(&conn)
            .unwrap_err()
            .to_string()
            .contains("identify a different session/trace/span"));
    }

    #[test]
    fn rejects_active_trace_identity_found_under_a_different_row_id() {
        let conn = source_connection("(3, 's1', 't1', 'p1', '[{\"name\":\"wrong row\"}]')");
        assert!(verify_event_source_identity(&conn)
            .unwrap_err()
            .to_string()
            .contains("under a different row id"));
    }

    #[test]
    fn rejects_duplicate_event_sources_for_one_row() {
        let conn = source_connection(
            "(1, 's1', 't1', 'p1', '[{\"name\":\"first\"}]'), (1, 's1', 't1', 'p1', '[{\"name\":\"second\"}]')",
        );
        assert!(verify_event_source_identity(&conn)
            .unwrap_err()
            .to_string()
            .contains("duplicate an active row id"));
    }

    #[test]
    fn rejects_payload_changes_after_rebuild() {
        let conn = source_connection("(1, 's1', 't1', 'p1', '[{\"name\":\"before\"}]')");
        conn.execute_batch(
            "CREATE TEMP TABLE trace_events_json AS SELECT * FROM trace_event_source; CREATE TEMP TABLE rebuilt AS SELECT _session_id AS session_id, _trace_id AS trace_id, _span_id AS span_id, '[{\"name\":\"after\"}]'::JSON AS events FROM trace_events_json;",
        )
        .unwrap();
        assert!(verify_event_payloads(&conn, "rebuilt", 1)
            .unwrap_err()
            .to_string()
            .contains("changed 1 event payloads"));
    }

    #[test]
    fn event_json_conversion_preserves_timestamp_nanoseconds() {
        let conn = Connection::open_in_memory().unwrap();
        let expression = events_json("[{name:'n', timestamp:TIMESTAMP_NS '2026-09-29 01:02:03.123456789', attributes:{a:'b'}}]");
        let value: String = conn
            .query_row(&format!("SELECT {expression}::VARCHAR"), [], |row| {
                row.get(0)
            })
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(&value).unwrap();
        assert_eq!(json[0]["timestamp"], "2026-09-29T01:02:03.123456789Z");
        assert_eq!(json[0]["attributes"]["a"], "b");
    }

    #[test]
    fn table_identifiers_must_be_plain_catalog_names() {
        assert_eq!(quote_ident("thelake").unwrap(), "\"thelake\"");
        assert!(quote_ident("thelake; DROP TABLE traces").is_err());
    }

    #[test]
    fn parquet_metadata_paths_resolve_as_relative_absolute_or_cloud_locations() {
        let conn = Connection::open_in_memory().unwrap();
        let expression = data_file_reference_expr("path", "/warehouse/thelake/traces/");
        let values: Vec<String> = {
            let mut statement = conn
                .prepare(&format!(
                    "SELECT {expression} FROM (VALUES ('partition/file.parquet'), ('/mnt/warehouse/file.parquet'), ('gs://bucket/warehouse/file.parquet')) AS paths(path)"
                ))
                .unwrap();
            statement
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        };
        assert_eq!(
            values,
            [
                "/warehouse/thelake/traces/partition/file.parquet",
                "/mnt/warehouse/file.parquet",
                "gs://bucket/warehouse/file.parquet",
            ]
        );
    }

    #[test]
    fn absolute_parquet_metadata_path_joins_and_returns_event_rows() {
        let conn = Connection::open_in_memory().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let parquet_path = directory.path().join("trace-events.parquet");
        let path_literal = sql_literal(&parquet_path.to_string_lossy());
        conn.execute_batch(&format!(
            "COPY (SELECT 42 AS row_id_start, 'session' AS session_id, 'trace' AS trace_id, 'span' AS span_id, [{{name: 'event'}}] AS events) TO {path_literal} (FORMAT PARQUET);"
        ))
        .unwrap();
        let expression = data_file_reference_expr("metadata.path", "/unused/relative/prefix/");
        let query = format!(
            "SELECT p.row_id_start, array_length(p.events) FROM read_parquet([{path_literal}], filename=true) p JOIN (SELECT {path_literal} AS path) metadata ON p.filename = {expression} WHERE p.events IS NOT NULL"
        );
        let (row_id, event_count): (i64, i64) = conn
            .query_row(&query, [], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        assert_eq!(row_id, 42);
        assert_eq!(event_count, 1);
    }
}
