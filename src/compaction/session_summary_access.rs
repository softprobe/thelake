//! Internal DuckDB access for session-summary maintenance.
//!
//! Reducers are maintenance work, not a second application-facing DuckDB
//! access mode. Only `MaintenanceEngine` reaches these helpers.

use crate::config::{Config, DuckLakeConfig};
use crate::session_summary::SummaryRow;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use duckdb::Connection;

/// Open and prepare a physical-scope maintenance connection without attaching.
/// Kept crate-visible for the focused object-store configuration test.
pub(crate) fn prepare_session_summary_duckdb(
    config: &Config,
    ducklake: &DuckLakeConfig,
) -> Result<Connection> {
    let access = crate::workspace_scope::DuckLakeAccess::Physical(
        crate::workspace_scope::PhysicalScope::from_ducklake(ducklake),
    );
    crate::storage::ducklake::DuckLakeSessionFactory::new(config)
        .open(
            &access,
            crate::storage::ducklake::DuckLakeSessionKind::Maintenance,
        )
        .context("open duckdb for session_summary reduce")
}

fn open_session_summary_connection(
    config: &Config,
    ducklake: &DuckLakeConfig,
) -> Result<Connection> {
    let conn = prepare_session_summary_duckdb(config, ducklake)?;
    let access = crate::workspace_scope::DuckLakeAccess::Physical(
        crate::workspace_scope::PhysicalScope::from_ducklake(ducklake),
    );
    crate::storage::ducklake::DuckLakeSessionFactory::new(config).attach(&conn, &access)?;
    Ok(conn)
}

fn micros_to_utc(us: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros(us)
}

fn map_duck_row(row: &duckdb::Row<'_>) -> duckdb::Result<SummaryRow> {
    let start_us: i64 = row.get(1)?;
    let end_us: Option<i64> = row.get(2)?;
    let start_time = micros_to_utc(start_us)
        .ok_or_else(|| duckdb::Error::InvalidParameterName(format!("start_time_us={start_us}")))?;
    let end_time = match end_us {
        Some(us) => Some(
            micros_to_utc(us)
                .ok_or_else(|| duckdb::Error::InvalidParameterName(format!("end_time_us={us}")))?,
        ),
        None => None,
    };
    Ok(SummaryRow {
        session_id: row.get(0)?,
        start_time,
        end_time,
        observation_count: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
        error_count: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
        input_tokens: row.get(5)?,
        output_tokens: row.get(6)?,
        total_tokens: row.get(7)?,
        total_cost: row.get(8)?,
        agent_name: row.get(9)?,
        user_id: row.get(10)?,
        model_name: row.get(11)?,
    })
}

/// Execute the promoted-only session aggregate on a physical maintenance
/// connection. Callers supply a typed window, never a connection or catalog
/// alias.
pub(crate) fn aggregate_sessions_from_lake(
    config: &Config,
    ducklake: &DuckLakeConfig,
    session_ids: Option<&[String]>,
    workspace_id: Option<&str>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<SummaryRow>> {
    let from_table = crate::storage::ducklake::ducklake_qualified_table_name(ducklake, "traces");
    let sql = match session_ids {
        Some(ids) if workspace_id.is_some() => {
            crate::sql::session_summary::compile_session_summary_reduce_sql_for_workspace(
                &from_table,
                ids,
                workspace_id.expect("checked above"),
                from,
                to,
            )?
        }
        Some(ids) => crate::sql::session_summary::compile_session_summary_reduce_sql(
            &from_table,
            ids,
            from,
            to,
        )?,
        None => crate::sql::session_summary::compile_session_summary_rebuild_sql_for_workspace(
            &from_table,
            workspace_id,
            from,
            to,
        )?,
    };
    let conn = open_session_summary_connection(config, ducklake)?;
    let mut stmt = conn.prepare(&sql).context("prepare aggregate SQL")?;
    stmt.query_map([], map_duck_row)
        .context("query aggregate")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("map aggregate rows")
}
