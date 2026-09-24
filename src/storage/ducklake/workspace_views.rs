use super::ducklake_qualified_table_name;
use super::util::escape_sql_literal;
use super::PhysicalScope;
use anyhow::{Context, Result};
use duckdb::Connection;

const WORKSPACE_TABLES: [(&str, &str); 4] = [
    ("traces", "traces"),
    ("logs", "logs"),
    ("scores", "scores"),
    ("score_configs", "score_configs"),
];

pub(crate) fn create_view_sql(
    scope: &PhysicalScope,
    logical_name: &str,
    physical_name: &str,
    workspace_id: &str,
) -> String {
    let physical_table = ducklake_qualified_table_name(scope, physical_name);
    format!(
        "CREATE OR REPLACE TEMP VIEW {logical_name} AS SELECT * FROM {physical_table} WHERE tenant_id = '{workspace_id}';",
        workspace_id = escape_sql_literal(workspace_id),
    )
}

fn create_fail_closed_view_sql(
    scope: &PhysicalScope,
    logical_name: &str,
    physical_name: &str,
) -> String {
    let physical_table = ducklake_qualified_table_name(scope, physical_name);
    format!(
        "CREATE OR REPLACE TEMP VIEW {logical_name} AS SELECT * FROM {physical_table} WHERE 1 = 0;"
    )
}

fn has_tenant_id_column(
    conn: &Connection,
    scope: &PhysicalScope,
    physical_name: &str,
) -> Result<bool> {
    let physical_table = ducklake_qualified_table_name(scope, physical_name);
    let mut statement = conn
        .prepare(&format!("DESCRIBE {physical_table}"))
        .with_context(|| format!("inspect shared workspace table {physical_name}"))?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        if name == "tenant_id" {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Verify that every physical table required by shared workspace access carries
/// the ownership column before any filtered view can be trusted.
pub(crate) fn validate_shared_workspace_schema(
    conn: &Connection,
    scope: &PhysicalScope,
) -> Result<()> {
    for (_, physical_name) in WORKSPACE_TABLES {
        if !has_tenant_id_column(conn, scope, physical_name)? {
            return Err(anyhow::anyhow!(
                "{}: table {physical_name} is missing tenant_id",
                super::SharedScopeError::new(
                    super::SharedScopeErrorCode::SchemaIncompatible,
                    format!("shared workspace table {physical_name} has no ownership column"),
                )
            ));
        }
    }
    Ok(())
}

pub(crate) fn install(conn: &Connection, scope: &PhysicalScope, workspace_id: &str) -> Result<()> {
    for (logical_name, physical_name) in WORKSPACE_TABLES {
        // Until the shared schema migration adds ownership columns, expose an
        // empty view rather than risking an unfiltered physical-table read.
        let sql = if has_tenant_id_column(conn, scope, physical_name)? {
            create_view_sql(scope, logical_name, physical_name, workspace_id)
        } else {
            create_fail_closed_view_sql(scope, logical_name, physical_name)
        };
        conn.execute_batch(&sql)
            .with_context(|| format!("create shared workspace view {logical_name}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::PhysicalScope;
    use super::*;

    #[test]
    fn view_sql_filters_the_physical_table_by_escaped_workspace() {
        let scope = PhysicalScope::new(
            "host=localhost dbname=ducklake",
            "s3://warehouse/shared/",
            "softprobe",
            "shared_scope",
        );

        let sql = create_view_sql(&scope, "traces", "traces", "workspace'42");

        assert_eq!(
            sql,
            "CREATE OR REPLACE TEMP VIEW traces AS SELECT * FROM softprobe.shared_scope.traces WHERE tenant_id = 'workspace''42';"
        );
    }

    #[test]
    fn all_workspace_tables_have_logical_views() {
        assert_eq!(WORKSPACE_TABLES.len(), 4);
        assert!(WORKSPACE_TABLES.contains(&("traces", "traces")));
        assert!(WORKSPACE_TABLES.contains(&("logs", "logs")));
        assert!(WORKSPACE_TABLES.contains(&("scores", "scores")));
        assert!(WORKSPACE_TABLES.contains(&("score_configs", "score_configs")));
    }

    #[test]
    fn missing_ownership_columns_use_a_fail_closed_view() {
        let scope = PhysicalScope::default();
        let sql = create_fail_closed_view_sql(&scope, "logs", "logs");
        assert!(sql.contains("WHERE 1 = 0"));
        assert!(!sql.contains("tenant_id"));
    }

    #[test]
    fn schema_compatibility_requires_ownership_on_every_table() {
        let scope = fixture_scope();
        let connection = fixture_connection();
        validate_shared_workspace_schema(&connection, &scope).expect("ownership columns");

        connection
            .execute_batch("ALTER TABLE softprobe.logs DROP COLUMN tenant_id;")
            .expect("drop ownership column");
        let error = validate_shared_workspace_schema(&connection, &scope)
            .expect_err("missing ownership column must fail closed");
        assert!(error
            .to_string()
            .contains("shared_scope_schema_incompatible"));
        assert!(error.to_string().contains("logs"));
    }

    #[test]
    fn fresh_connections_filter_all_workspace_tables() {
        let scope = fixture_scope();
        for connection_kind in ["worker-start", "worker-rebuild", "one-shot"] {
            let connection = fixture_connection();
            install(&connection, &scope, "workspace-a")
                .unwrap_or_else(|error| panic!("{connection_kind} view install: {error}"));

            for table in ["traces", "logs", "scores", "score_configs"] {
                let count: i64 = connection
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap_or_else(|error| panic!("{connection_kind} {table}: {error}"));
                assert_eq!(count, 1, "{connection_kind} must expose workspace-a only");
            }

            let visible_id: String = connection
                .query_row("SELECT id FROM traces", [], |row| row.get(0))
                .unwrap();
            assert_eq!(visible_id, "workspace-a-row");
        }

        let other_connection = fixture_connection();
        install(&other_connection, &scope, "workspace-b").unwrap();
        let visible_id: String = other_connection
            .query_row("SELECT id FROM traces", [], |row| row.get(0))
            .unwrap();
        assert_eq!(visible_id, "workspace-b-row");
    }

    fn fixture_scope() -> PhysicalScope {
        PhysicalScope::new(
            "host=localhost dbname=ducklake",
            "./warehouse/ducklake/data/",
            "softprobe",
            "main",
        )
    }

    fn fixture_connection() -> Connection {
        let connection = Connection::open_in_memory().expect("open fixture connection");
        connection
            .execute_batch(
                "ATTACH ':memory:' AS softprobe;
                 CREATE TABLE softprobe.traces (tenant_id VARCHAR, id VARCHAR);
                 CREATE TABLE softprobe.logs (tenant_id VARCHAR, id VARCHAR);
                 CREATE TABLE softprobe.scores (tenant_id VARCHAR, id VARCHAR);
                 CREATE TABLE softprobe.score_configs (tenant_id VARCHAR, id VARCHAR);
                 INSERT INTO softprobe.traces VALUES
                   ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
                 INSERT INTO softprobe.logs VALUES
                   ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
                 INSERT INTO softprobe.scores VALUES
                   ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');
                 INSERT INTO softprobe.score_configs VALUES
                   ('workspace-a', 'workspace-a-row'), ('workspace-b', 'workspace-b-row');",
            )
            .expect("seed fixture tables");
        connection
    }
}
