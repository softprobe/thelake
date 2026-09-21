//! DuckLake execution helpers for the metrics registry.
//!
//! Table ownership and DDL live in [`crate::sql::schema`]. This module only
//! performs readiness probes and additive upgrades against an attached catalog.

use anyhow::{anyhow, Result};
use duckdb::Connection;

use super::ducklake_partition::{describe_table_columns, table_partition_sort_ready};
use crate::sql::schema::{create_table_sql, ensure_table_sql, partition_sort_sql, TableSpec};

pub type MetricsLayoutTable = TableSpec;

pub use crate::sql::schema::{
    METRICS_LAYOUT_COLLAPSE_TABLES, METRICS_LAYOUT_CORE_TABLES, METRICS_LAYOUT_DOWNSAMPLE_TABLES,
};

pub fn qualified_metrics_layout_table(catalog_alias: &str, table_name: &str) -> String {
    format!("{catalog_alias}.{table_name}")
}

pub fn create_metrics_layout_table_sql(catalog_alias: &str, table: &MetricsLayoutTable) -> String {
    create_table_sql(catalog_alias, table)
}

pub fn apply_metrics_layout_partition_sort_sql(
    catalog_alias: &str,
    table: &MetricsLayoutTable,
) -> String {
    partition_sort_sql(catalog_alias, table)
}

pub fn ensure_metrics_layout_table_sql(catalog_alias: &str, table: &MetricsLayoutTable) -> String {
    ensure_table_sql(catalog_alias, table)
}

fn metrics_layout_table_ready(
    conn: &Connection,
    catalog_alias: &str,
    table_name: &str,
) -> Result<bool> {
    table_partition_sort_ready(conn, catalog_alias, table_name)
}

pub fn ensure_metrics_layout_table(
    conn: &Connection,
    catalog_alias: &str,
    table: &MetricsLayoutTable,
) -> Result<()> {
    if !metrics_layout_table_ready(conn, catalog_alias, table.name)? {
        let sql = ensure_metrics_layout_table_sql(catalog_alias, table);
        conn.execute_batch(&sql).map_err(|e| {
            anyhow!(
                "failed to ensure metrics layout table {}.{}: {e}",
                catalog_alias,
                table.name
            )
        })?;
    }
    ensure_layout_additive_columns(conn, catalog_alias, table.name)?;
    if table.name == "metric_series" {
        ensure_metric_series_labels_are_map(conn, catalog_alias)?;
    }
    Ok(())
}

pub fn ensure_metrics_layout_core_tables(conn: &Connection, catalog_alias: &str) -> Result<()> {
    for table in METRICS_LAYOUT_CORE_TABLES {
        ensure_metrics_layout_table(conn, catalog_alias, table)?;
    }
    Ok(())
}

pub fn metrics_layout_family_tables() -> Vec<&'static MetricsLayoutTable> {
    METRICS_LAYOUT_CORE_TABLES
        .iter()
        .chain(METRICS_LAYOUT_DOWNSAMPLE_TABLES)
        .chain(METRICS_LAYOUT_COLLAPSE_TABLES)
        .collect()
}

pub fn ensure_metrics_layout_family_tables(conn: &Connection, catalog_alias: &str) -> Result<()> {
    for table in metrics_layout_family_tables() {
        ensure_metrics_layout_table(conn, catalog_alias, table)?;
    }
    Ok(())
}

pub fn apply_metrics_layout_partition_sort(
    conn: &Connection,
    catalog_alias: &str,
    table: &MetricsLayoutTable,
) -> Result<()> {
    conn.execute_batch(&apply_metrics_layout_partition_sort_sql(
        catalog_alias,
        table,
    ))
    .map_err(|e| {
        anyhow!(
            "failed to apply partition/sort on {}.{}: {e}",
            catalog_alias,
            table.name
        )
    })?;
    Ok(())
}

fn ensure_metric_series_labels_are_map(conn: &Connection, catalog_alias: &str) -> Result<()> {
    let qualified = qualified_metrics_layout_table(catalog_alias, "metric_series");
    let found = describe_table_columns(conn, &qualified)?;
    let Some(dtype) = found.get("labels") else {
        return Err(anyhow!(
            "table {qualified} is missing required MAP column 'labels'"
        ));
    };
    let upper = dtype.to_ascii_uppercase();
    if upper == "VARIANT" || upper.starts_with("VARIANT") {
        return Err(anyhow!(
            "table {qualified} column 'labels' has type {dtype} (VARIANT). \
             Rebuild/migrate this DuckLake table via operations, then re-ingest."
        ));
    }
    if !upper.contains("MAP") {
        return Err(anyhow!(
            "table {qualified} column 'labels' has type {dtype}, expected \
             MAP(VARCHAR, VARCHAR). Rebuild/migrate this DuckLake table via operations, \
             then re-ingest."
        ));
    }
    Ok(())
}

fn ensure_layout_additive_columns(
    conn: &Connection,
    catalog_alias: &str,
    table_name: &str,
) -> Result<()> {
    let needed: &[(&str, &str)] = match table_name {
        "metric_series" => &[
            ("aggregation_temporality", "VARCHAR"),
            ("is_monotonic", "BOOLEAN"),
        ],
        "metric_hist_samples" => &[("quantiles", "VARCHAR"), ("exemplars_json", "VARCHAR")],
        _ => return Ok(()),
    };
    let qualified = qualified_metrics_layout_table(catalog_alias, table_name);
    let found = describe_table_columns(conn, &qualified)?;
    let ddls = needed
        .iter()
        .filter(|(name, _)| !found.contains_key(*name))
        .map(|(name, sql_type)| crate::sql::schema::add_column_sql(&qualified, name, sql_type))
        .collect::<Vec<_>>();
    if !ddls.is_empty() {
        conn.execute_batch(&ddls.join("\n"))
            .map_err(|e| anyhow!("failed to add additive columns on {qualified}: {e}"))?;
    }
    Ok(())
}

pub fn union_metrics_from_layout_sql(catalog_prefix: &str) -> String {
    crate::sql::schema::union_metrics_sql(catalog_prefix)
}

pub fn union_metrics_layout_relation_sql(catalog_prefix: &str, relation_alias: &str) -> String {
    format!(
        "({inner}) AS {relation_alias}",
        inner = union_metrics_from_layout_sql(catalog_prefix)
    )
}
