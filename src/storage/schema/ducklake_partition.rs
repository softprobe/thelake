//! Shared DuckLake partition/sort readiness probe (metrics + OTLP tables).

use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

static DESCRIBE_PROBE_COUNT: AtomicUsize = AtomicUsize::new(0);
static PARTITION_SORT_PROBE_COUNT: AtomicUsize = AtomicUsize::new(0);

pub fn describe_probe_count() -> usize {
    DESCRIBE_PROBE_COUNT.load(Ordering::Relaxed)
}

pub fn partition_sort_probe_count() -> usize {
    PARTITION_SORT_PROBE_COUNT.load(Ordering::Relaxed)
}

pub fn total_schema_probe_count() -> usize {
    describe_probe_count() + partition_sort_probe_count()
}

/// Consolidated DESCRIBE table helper tracking probes. Returns lowercase column names.
pub(crate) fn describe_table_columns(
    conn: &Connection,
    qualified_table: &str,
) -> Result<HashMap<String, String>> {
    DESCRIBE_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);
    let sql = format!("DESCRIBE {qualified_table};");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| anyhow!("DESCRIBE {qualified_table} failed: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let dtype: String = row.get(1)?;
            Ok((name, dtype))
        })
        .map_err(|e| anyhow!("DESCRIBE {qualified_table} query failed: {e}"))?;

    let mut found = HashMap::new();
    for row in rows {
        let (name, dtype) = row.map_err(|e| anyhow!("DESCRIBE row failed: {e}"))?;
        found.insert(name.to_ascii_lowercase(), dtype);
    }
    Ok(found)
}

/// Whether the table's active partition and sort definitions match the shared
/// OTLP profile. A merely-present definition is insufficient for existing logs
/// and scores whose historical sort tuple may differ.
pub(crate) fn table_partition_sort_matches(
    conn: &Connection,
    catalog_or_qualified: &str,
    table_name: &str,
    partition_by: &str,
    sorted_by: &str,
) -> Result<bool> {
    PARTITION_SORT_PROBE_COUNT.fetch_add(1, Ordering::Relaxed);
    let attach = catalog_or_qualified
        .split('.')
        .next()
        .unwrap_or(catalog_or_qualified);
    let qualified_parts = catalog_or_qualified.split('.').collect::<Vec<_>>();
    let schema_name = if qualified_parts.len() >= 3 {
        qualified_parts[qualified_parts.len() - 2]
    } else {
        "main"
    };
    let meta = format!("__ducklake_metadata_{attach}");
    let sql = format!(
        "SELECT \
           COALESCE((SELECT string_agg( \
             CASE WHEN pc.transform = 'identity' THEN c.column_name \
                  ELSE pc.transform || '(' || c.column_name || ')' END, \
             ', ' ORDER BY pc.partition_key_index) \
             FROM {meta}.ducklake_partition_info pi \
             JOIN {meta}.ducklake_table t ON t.table_id = pi.table_id \
             JOIN {meta}.ducklake_schema s ON s.schema_id = t.schema_id \
             JOIN {meta}.ducklake_partition_column pc \
               ON pc.partition_id = pi.partition_id AND pc.table_id = pi.table_id \
             JOIN {meta}.ducklake_column c \
               ON c.column_id = pc.column_id AND c.table_id = pc.table_id \
             WHERE t.table_name = ? AND s.schema_name = ? AND t.end_snapshot IS NULL \
               AND pi.end_snapshot IS NULL AND c.end_snapshot IS NULL), ''), \
           COALESCE((SELECT string_agg(se.expression, ', ' ORDER BY se.sort_key_index) \
             FROM {meta}.ducklake_sort_info si \
             JOIN {meta}.ducklake_table t ON t.table_id = si.table_id \
             JOIN {meta}.ducklake_schema s ON s.schema_id = t.schema_id \
             JOIN {meta}.ducklake_sort_expression se \
               ON se.sort_id = si.sort_id AND se.table_id = si.table_id \
             WHERE t.table_name = ? AND s.schema_name = ? AND t.end_snapshot IS NULL \
               AND si.end_snapshot IS NULL), '')"
    );
    let (actual_partition, actual_sort): (String, String) = conn
        .query_row(
            &sql,
            [table_name, schema_name, table_name, schema_name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| anyhow!("read active layout metadata for {table_name}: {error}"))?;
    let normalize = |value: &str| {
        value
            .chars()
            .filter(|character| !character.is_whitespace() && *character != '"')
            .flat_map(char::to_lowercase)
            .collect::<String>()
    };
    Ok(normalize(&actual_partition) == normalize(partition_by)
        && normalize(&actual_sort) == normalize(sorted_by))
}
