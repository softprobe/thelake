use crate::config::Config;
use crate::sql::maintenance::migrate_variant as sql_templates;
use crate::storage::ducklake::{configure_object_store, escape_sql_literal};
use crate::storage::schema::attribute_map::is_variant_column;
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use duckdb::Connection;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio_postgres::{Client, NoTls};
use tracing::info;

const TELEMETRY_TABLES: &[&str] = &["traces", "logs", "scores", "score_configs"];
const OPERATIONAL_PG_TABLES: &[&str] = &[
    "promotion_specs",
    "capture_session_mappings",
    "session_summary",
    "session_summary_dirty",
    "tag_index",
    "maintenance_scope_config",
];

#[derive(Debug, Parser)]
#[command(
    name = "thelake migrate-variant",
    about = "Migrate workspaces, operational Postgres tables, and DuckLake telemetry from a MAP(VARCHAR, VARCHAR) schema to a DuckLake 1.1 VARIANT schema"
)]
pub struct MigrateVariantArgs {
    /// Source Postgres metadata schema (for example: thelake_staging or thelake).
    #[arg(long)]
    pub src_schema: String,
    /// Source DuckLake data path URI (for example: gs://softprobe-thelake/staging/shared/).
    #[arg(long)]
    pub src_data_path: String,
    /// Destination Postgres metadata schema (defaults to DUCKLAKE_METADATA_SCHEMA).
    #[arg(long)]
    pub dst_schema: Option<String>,
    /// Destination DuckLake data path URI (defaults to configured shared data path).
    #[arg(long)]
    pub dst_data_path: Option<String>,
    /// Print plan and counts without modifying destination tables.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    /// Migrate Postgres workspaces and operational tables only; skip DuckLake Parquet data copy.
    #[arg(long, default_value_t = false)]
    pub skip_data: bool,
}

#[derive(Debug, Clone)]
pub struct PostgresMigrationReport {
    pub workspaces_before: i64,
    pub workspaces_after: i64,
    pub src_workspaces: i64,
    pub operational_tables_copied: Vec<(String, u64)>,
}

#[derive(Debug, Clone)]
pub struct TableMigrationReport {
    pub table: String,
    pub src_rows: i64,
    pub dst_rows_before: i64,
    pub dst_rows_after: i64,
}

pub fn validate_pg_ident(ident: &str) -> Result<()> {
    if ident.is_empty()
        || !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || ident.chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        bail!("invalid Postgres identifier: {ident:?}");
    }
    Ok(())
}

pub fn build_column_projection(
    table_name: &str,
    src_cols: &[(String, String)],
    dst_cols: &[(String, String)],
) -> Result<(Vec<String>, Vec<String>)> {
    let src_map: HashMap<String, (&str, &str)> = src_cols
        .iter()
        .map(|(name, ty)| (name.to_ascii_lowercase(), (name.as_str(), ty.as_str())))
        .collect();

    let mut insert_cols = Vec::new();
    let mut select_exprs = Vec::new();

    for (dst_name, dst_type) in dst_cols {
        let lower = dst_name.to_ascii_lowercase();
        let Some((src_name, src_type)) = src_map.get(&lower) else {
            continue;
        };
        insert_cols.push(dst_name.clone());
        let needs_variant_cast = (dst_type.eq_ignore_ascii_case("VARIANT")
            || is_variant_column(table_name, &lower))
            && !src_type.eq_ignore_ascii_case("VARIANT");
        if needs_variant_cast {
            select_exprs.push(format!(
                "TRY_CAST({src_name} AS JSON)::VARIANT AS {dst_name}"
            ));
        } else {
            select_exprs.push((*src_name).to_string());
        }
    }

    if insert_cols.is_empty() {
        bail!("no common columns found for table {table_name}");
    }
    Ok((insert_cols, select_exprs))
}

pub async fn run_cli(args: MigrateVariantArgs) -> Result<()> {
    let config = Config::load()?;
    let dst_schema = args
        .dst_schema
        .unwrap_or_else(|| config.ducklake.metadata_schema.clone());
    let dst_data_path = args
        .dst_data_path
        .unwrap_or_else(|| config.ducklake.data_path.clone());
    let pg_dsn = config.ducklake.metadata_path.clone();

    validate_pg_ident(&args.src_schema)?;
    validate_pg_ident(&dst_schema)?;

    info!(
        src_schema = %args.src_schema,
        src_data_path = %args.src_data_path,
        dst_schema = %dst_schema,
        dst_data_path = %dst_data_path,
        dry_run = args.dry_run,
        skip_data = args.skip_data,
        "Starting DuckLake VARIANT migration"
    );

    let pg_report =
        migrate_postgres_metadata(&pg_dsn, &args.src_schema, &dst_schema, args.dry_run).await?;
    info!(
        src_workspaces = pg_report.src_workspaces,
        workspaces_before = pg_report.workspaces_before,
        workspaces_after = pg_report.workspaces_after,
        "Postgres workspace & metadata migration complete"
    );

    if !args.skip_data {
        let config_clone = config.clone();
        let src_schema = args.src_schema.clone();
        let src_data_path = args.src_data_path.clone();
        let dst_schema_clone = dst_schema.clone();
        let dst_data_path_clone = dst_data_path.clone();
        let dry_run = args.dry_run;
        let table_reports = tokio::task::spawn_blocking(move || {
            migrate_ducklake_tables(
                &config_clone,
                &pg_dsn,
                &src_schema,
                &src_data_path,
                &dst_schema_clone,
                &dst_data_path_clone,
                dry_run,
            )
        })
        .await??;

        for report in &table_reports {
            info!(
                table = %report.table,
                src_rows = report.src_rows,
                dst_rows_before = report.dst_rows_before,
                dst_rows_after = report.dst_rows_after,
                "DuckLake table migration result"
            );
        }
    }

    Ok(())
}

async fn table_columns(client: &Client, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows = client
        .query(sql_templates::pg_table_columns_sql(), &[&schema, &table])
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

pub async fn migrate_postgres_metadata(
    pg_dsn: &str,
    src_schema: &str,
    dst_schema: &str,
    dry_run: bool,
) -> Result<PostgresMigrationReport> {
    validate_pg_ident(src_schema)?;
    validate_pg_ident(dst_schema)?;

    let (client, connection) = tokio_postgres::connect(pg_dsn, NoTls)
        .await
        .context("connect to Postgres for migration")?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!("Postgres migration connection error: {err}");
        }
    });

    let dst_scope_row = client
        .query_opt(
            &sql_templates::render_pg_physical_scope_sql(dst_schema),
            &[],
        )
        .await
        .context("query destination physical_scope")?;
    let dst_physical_scope_id: String = dst_scope_row
        .ok_or_else(|| {
            anyhow!(
                "{dst_schema}.physical_scope is empty; boot thelake against {dst_schema} once before migrating"
            )
        })?
        .get(0);

    let src_workspaces: i64 = client
        .query_one(
            &sql_templates::render_pg_workspace_count_sql(src_schema),
            &[],
        )
        .await?
        .get(0);
    let workspaces_before: i64 = client
        .query_one(
            &sql_templates::render_pg_workspace_count_sql(dst_schema),
            &[],
        )
        .await?
        .get(0);

    if !dry_run {
        let src_ws_cols = table_columns(&client, src_schema, "workspace_scope_binding").await?;
        let dst_ws_cols: HashSet<String> =
            table_columns(&client, dst_schema, "workspace_scope_binding")
                .await?
                .into_iter()
                .collect();
        let common_cols: Vec<String> = src_ws_cols
            .into_iter()
            .filter(|c| dst_ws_cols.contains(c))
            .collect();
        if !common_cols.iter().any(|c| c == "workspace_id") {
            bail!("workspace_scope_binding missing workspace_id column");
        }
        let insert_list = common_cols.join(", ");
        let select_list = common_cols
            .iter()
            .map(|c| {
                if c == "physical_scope_id" {
                    "$1::text AS physical_scope_id".to_string()
                } else {
                    c.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = sql_templates::render_pg_copy_workspaces_sql(
            src_schema,
            dst_schema,
            &insert_list,
            &select_list,
        );
        client.execute(&sql, &[&dst_physical_scope_id]).await?;

        let missing: i64 = client
            .query_one(
                &sql_templates::render_pg_missing_workspaces_sql(src_schema, dst_schema),
                &[],
            )
            .await?
            .get(0);
        if missing > 0 {
            bail!("{missing} workspaces from {src_schema} are still missing in {dst_schema}");
        }
    }

    let workspaces_after: i64 = client
        .query_one(
            &sql_templates::render_pg_workspace_count_sql(dst_schema),
            &[],
        )
        .await?
        .get(0);

    let mut operational_tables_copied = Vec::new();
    for table in OPERATIONAL_PG_TABLES {
        let src_cols = table_columns(&client, src_schema, table).await?;
        let dst_cols: HashSet<String> = table_columns(&client, dst_schema, table)
            .await?
            .into_iter()
            .collect();
        if src_cols.is_empty() || dst_cols.is_empty() {
            continue;
        }
        let common_cols: Vec<String> = src_cols
            .into_iter()
            .filter(|c| dst_cols.contains(c))
            .collect();
        if common_cols.is_empty() {
            continue;
        }
        if dry_run {
            let count: i64 = client
                .query_one(
                    &sql_templates::render_table_count_sql(src_schema, table),
                    &[],
                )
                .await?
                .get(0);
            operational_tables_copied.push(((*table).to_string(), count.max(0) as u64));
            continue;
        }

        let insert_list = common_cols.join(", ");
        let uses_scope_id = common_cols.iter().any(|c| c == "physical_scope_id");
        let select_list = common_cols
            .iter()
            .map(|c| {
                if c == "physical_scope_id" {
                    "$1::text AS physical_scope_id".to_string()
                } else {
                    c.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = sql_templates::render_pg_copy_table_sql(
            src_schema,
            dst_schema,
            table,
            &insert_list,
            &select_list,
        );
        let inserted = if uses_scope_id {
            client.execute(&sql, &[&dst_physical_scope_id]).await?
        } else {
            client.execute(&sql, &[]).await?
        };
        operational_tables_copied.push(((*table).to_string(), inserted));
    }

    Ok(PostgresMigrationReport {
        workspaces_before,
        workspaces_after,
        src_workspaces,
        operational_tables_copied,
    })
}

fn describe_duckdb_table(
    conn: &Connection,
    qualified: &str,
) -> Result<Option<Vec<(String, String)>>> {
    let parts: Vec<&str> = qualified.split('.').collect();
    if parts.len() == 3 {
        let exists: i64 = conn.query_row(
            sql_templates::ducklake_table_exists_sql(),
            [parts[0], parts[1], parts[2]],
            |r| r.get(0),
        )?;
        if exists == 0 {
            return Ok(None);
        }
    }
    let mut stmt = conn
        .prepare(&format!("DESCRIBE {qualified}"))
        .with_context(|| format!("describe {qualified}"))?;
    let mut rows = stmt.query([])?;
    let mut cols = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let ty: String = row.get(1)?;
        cols.push((name, ty));
    }
    Ok(Some(cols))
}

pub fn migrate_ducklake_tables(
    config: &Config,
    pg_dsn: &str,
    src_schema: &str,
    src_data_path: &str,
    dst_schema: &str,
    dst_data_path: &str,
    dry_run: bool,
) -> Result<Vec<TableMigrationReport>> {
    validate_pg_ident(src_schema)?;
    validate_pg_ident(dst_schema)?;

    let duckdb_cfg = duckdb::Config::default()
        .allow_unsigned_extensions()
        .context("enable unsigned extensions for ducklake")?;
    let conn =
        Connection::open_in_memory_with_flags(duckdb_cfg).context("open DuckDB for migration")?;
    let ext_path = Path::new(&config.ducklake.extension_path);
    conn.execute_batch(&sql_templates::render_ducklake_init_sql(
        &escape_sql_literal(&ext_path.to_string_lossy()),
    ))
    .context("load ducklake extension")?;
    configure_object_store(&conn, config, src_data_path).context("configure src object store")?;
    if dst_data_path != src_data_path {
        configure_object_store(&conn, config, dst_data_path)
            .context("configure dst object store")?;
    }

    conn.execute_batch(&sql_templates::render_ducklake_attach_sql(
        &escape_sql_literal(pg_dsn),
        &escape_sql_literal(src_schema),
        &escape_sql_literal(src_data_path),
        &escape_sql_literal(dst_schema),
        &escape_sql_literal(dst_data_path),
    ))
    .context("open src_lake and dst_lake")?;

    let src_qualifier = format!("src_lake.{src_schema}");
    let dst_qualifier = format!("dst_lake.{dst_schema}");

    let mut reports = Vec::new();
    for table in TELEMETRY_TABLES {
        let src_table = format!("{src_qualifier}.{table}");
        let dst_table = format!("{dst_qualifier}.{table}");
        let Some(src_cols) = describe_duckdb_table(&conn, &src_table)? else {
            continue;
        };
        let Some(dst_cols) = describe_duckdb_table(&conn, &dst_table)? else {
            bail!("destination table {dst_table} does not exist");
        };

        let src_rows: i64 = conn.query_row(
            &sql_templates::render_table_count_sql(&src_qualifier, table),
            [],
            |r| r.get(0),
        )?;
        let dst_rows_before: i64 = conn.query_row(
            &sql_templates::render_table_count_sql(&dst_qualifier, table),
            [],
            |r| r.get(0),
        )?;

        if dry_run || src_rows == 0 {
            reports.push(TableMigrationReport {
                table: (*table).to_string(),
                src_rows,
                dst_rows_before,
                dst_rows_after: dst_rows_before,
            });
            continue;
        }

        let (insert_cols, select_exprs) = build_column_projection(table, &src_cols, &dst_cols)?;
        let insert_cols_sql = insert_cols.join(", ");
        let select_exprs_sql = select_exprs.join(", ");

        let has_timestamp = src_cols
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("timestamp"));
        if has_timestamp && src_rows > 50_000 {
            let mut day_stmt = conn.prepare(&sql_templates::render_ducklake_distinct_days_sql(
                &src_table,
            ))?;
            let mut day_rows = day_stmt.query([])?;
            let mut days = Vec::new();
            while let Some(row) = day_rows.next()? {
                let day: String = row.get(0)?;
                days.push(day);
            }

            for day in days {
                let day_start = format!("{day}T00:00:00Z");
                let dst_day_count: i64 = conn.query_row(
                    &sql_templates::render_ducklake_day_count_sql(&dst_table, &day_start),
                    [],
                    |r| r.get(0),
                )?;
                let dedup_traces = *table == "traces" && dst_day_count > 0;
                if *table != "traces" && dst_day_count > 0 {
                    continue;
                }

                info!(table = %table, day = %day, dst_day_count, "Migrating DuckLake day partition");
                conn.execute_batch(&sql_templates::render_ducklake_insert_day_sql(
                    &src_table,
                    &dst_table,
                    &insert_cols_sql,
                    &select_exprs_sql,
                    &day_start,
                    dedup_traces,
                ))
                .with_context(|| format!("migrate {table} for day {day}"))?;
            }
        } else if dst_rows_before == 0 {
            conn.execute_batch(&sql_templates::render_ducklake_insert_all_sql(
                &src_table,
                &dst_table,
                &insert_cols_sql,
                &select_exprs_sql,
            ))
            .with_context(|| format!("migrate {table}"))?;
        }

        let dst_rows_after: i64 = conn.query_row(
            &sql_templates::render_table_count_sql(&dst_qualifier, table),
            [],
            |r| r.get(0),
        )?;
        if dst_rows_after < src_rows {
            bail!(
                "migration incomplete for {table}: dst_rows_after ({dst_rows_after}) < src_rows ({src_rows})"
            );
        }

        reports.push(TableMigrationReport {
            table: (*table).to_string(),
            src_rows,
            dst_rows_before,
            dst_rows_after,
        });
    }

    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_column_projection_casts_map_columns_to_variant() {
        let src = vec![
            ("trace_id".to_string(), "VARCHAR".to_string()),
            (
                "attributes".to_string(),
                "MAP(VARCHAR, VARCHAR)".to_string(),
            ),
            (
                "resource_attributes".to_string(),
                "MAP(VARCHAR, VARCHAR)".to_string(),
            ),
        ];
        let dst = vec![
            ("trace_id".to_string(), "VARCHAR".to_string()),
            ("attributes".to_string(), "VARIANT".to_string()),
            ("resource_attributes".to_string(), "VARIANT".to_string()),
        ];

        let (insert_cols, select_exprs) =
            build_column_projection("traces", &src, &dst).expect("projection");
        assert_eq!(
            insert_cols,
            vec!["trace_id", "attributes", "resource_attributes"]
        );
        assert_eq!(
            select_exprs,
            vec![
                "trace_id",
                "TRY_CAST(attributes AS JSON)::VARIANT AS attributes",
                "TRY_CAST(resource_attributes AS JSON)::VARIANT AS resource_attributes",
            ]
        );
    }

    #[test]
    fn duckdb_map_to_variant_roundtrip_preserves_rows() {
        let conn = Connection::open_in_memory().expect("duckdb");
        conn.execute_batch(
            "CREATE TABLE src_traces (
                trace_id VARCHAR,
                span_id VARCHAR,
                attributes MAP(VARCHAR, VARCHAR),
                resource_attributes MAP(VARCHAR, VARCHAR)
             );
             CREATE TABLE dst_traces (
                trace_id VARCHAR,
                span_id VARCHAR,
                attributes VARIANT,
                resource_attributes VARIANT
             );
             INSERT INTO src_traces VALUES (
                't1',
                's1',
                MAP {'http.method': 'GET', 'session.id': 'ses_1'},
                MAP {'service.name': 'travel-ota'}
             );",
        )
        .expect("seed tables");

        let src_cols = describe_duckdb_table(&conn, "src_traces").unwrap().unwrap();
        let dst_cols = describe_duckdb_table(&conn, "dst_traces").unwrap().unwrap();
        let (insert_cols, select_exprs) =
            build_column_projection("traces", &src_cols, &dst_cols).expect("projection");

        conn.execute_batch(&format!(
            "INSERT INTO dst_traces ({}) SELECT {} FROM src_traces;",
            insert_cols.join(", "),
            select_exprs.join(", ")
        ))
        .expect("migrate map to variant");

        let (count, method, service): (i64, String, String) = conn
            .query_row(
                "SELECT COUNT(*),
                        CAST(attributes['http.method'] AS VARCHAR),
                        CAST(resource_attributes['service.name'] AS VARCHAR)
                 FROM dst_traces
                 GROUP BY 2, 3",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("query migrated variant");
        assert_eq!(count, 1);
        assert_eq!(method, "GET");
        assert_eq!(service, "travel-ota");
    }

    #[test]
    fn schema_qualified_duckdb_tables_describe_and_dedup_insert() {
        let conn = Connection::open_in_memory().expect("duckdb");
        conn.execute_batch(
            "ATTACH ':memory:' AS src_lake;
             ATTACH ':memory:' AS dst_lake;
             CREATE SCHEMA src_lake.thelake_staging;
             CREATE SCHEMA dst_lake.thelake_staging_ducklake11;
             CREATE TABLE src_lake.thelake_staging.traces (
                trace_id VARCHAR,
                span_id VARCHAR,
                timestamp TIMESTAMP_NS,
                attributes MAP(VARCHAR, VARCHAR)
             );
             CREATE TABLE dst_lake.thelake_staging_ducklake11.traces (
                trace_id VARCHAR,
                span_id VARCHAR,
                timestamp TIMESTAMP_NS,
                attributes VARIANT
             );
             INSERT INTO src_lake.thelake_staging.traces VALUES
                ('t1', 's1', '2026-11-16T10:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, MAP {'a': '1'}),
                ('t2', 's2', '2026-11-16T11:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, MAP {});
             INSERT INTO dst_lake.thelake_staging_ducklake11.traces VALUES
                ('t1', 's1', '2026-11-16T10:00:00Z'::TIMESTAMPTZ::TIMESTAMP_NS, '{\"a\":\"1\"}'::VARIANT);",
        )
        .expect("seed attached schemas");

        let src_table = "src_lake.thelake_staging.traces";
        let dst_table = "dst_lake.thelake_staging_ducklake11.traces";
        let src_cols = describe_duckdb_table(&conn, src_table).unwrap().unwrap();
        let dst_cols = describe_duckdb_table(&conn, dst_table).unwrap().unwrap();
        assert!(
            describe_duckdb_table(&conn, "src_lake.thelake_staging.missing")
                .unwrap()
                .is_none()
        );

        let (insert_cols, select_exprs) =
            build_column_projection("traces", &src_cols, &dst_cols).expect("projection");
        let day_start = "2026-11-16T00:00:00Z";
        let sql = sql_templates::render_ducklake_insert_day_sql(
            src_table,
            dst_table,
            &insert_cols.join(", "),
            &select_exprs.join(", "),
            day_start,
            true,
        );
        conn.execute_batch(&sql).expect("dedup day insert");

        let dst_count: i64 = conn
            .query_row(
                &sql_templates::render_ducklake_day_count_sql(dst_table, day_start),
                [],
                |r| r.get(0),
            )
            .expect("count dst");
        assert_eq!(dst_count, 2);
    }
}
