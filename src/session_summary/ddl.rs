//! Per-tenant Postgres DDL for `session_summary` + `session_summary_dirty`.

use crate::runtime_engine::quote_pg_ident;

/// Legacy per-workspace DDL for isolated scopes.
pub fn session_summary_table_ddls(tenant_schema: &str) -> Vec<String> {
    let schema = quote_pg_ident(tenant_schema);
    vec![
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary (
  session_id          TEXT        NOT NULL,
  start_time          TIMESTAMPTZ NOT NULL,
  end_time            TIMESTAMPTZ,
  observation_count   BIGINT      NOT NULL DEFAULT 0,
  error_count         BIGINT      NOT NULL DEFAULT 0,
  input_tokens        BIGINT,
  output_tokens       BIGINT,
  total_tokens        BIGINT,
  total_cost          DOUBLE PRECISION,
  agent_name          TEXT,
  user_id             TEXT,
  model_name          TEXT,
  updated_at          TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (session_id)
);"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_recent
  ON {schema}.session_summary (start_time DESC, session_id);"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_agent
  ON {schema}.session_summary (agent_name, start_time DESC, session_id)
  WHERE agent_name IS NOT NULL;"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_errors
  ON {schema}.session_summary (start_time DESC, session_id)
  WHERE error_count > 0;"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary_dirty (
  session_id   TEXT        NOT NULL,
  min_ts       TIMESTAMPTZ NOT NULL,
  max_ts       TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (session_id)
);"#
        ),
    ]
}

/// Composite-key DDL for a shared physical scope.
pub fn shared_session_summary_table_ddls(tenant_schema: &str) -> Vec<String> {
    let schema = quote_pg_ident(tenant_schema);
    vec![
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary (
  tenant_id           TEXT        NOT NULL,
  session_id          TEXT        NOT NULL,
  start_time          TIMESTAMPTZ NOT NULL,
  end_time            TIMESTAMPTZ,
  observation_count   BIGINT      NOT NULL DEFAULT 0,
  error_count         BIGINT      NOT NULL DEFAULT 0,
  input_tokens        BIGINT,
  output_tokens       BIGINT,
  total_tokens        BIGINT,
  total_cost          DOUBLE PRECISION,
  agent_name          TEXT,
  user_id             TEXT,
  model_name          TEXT,
  updated_at          TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, session_id)
);"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_recent
  ON {schema}.session_summary (tenant_id, start_time DESC, session_id);"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_agent
  ON {schema}.session_summary (tenant_id, agent_name, start_time DESC, session_id)
  WHERE agent_name IS NOT NULL;"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_errors
  ON {schema}.session_summary (tenant_id, start_time DESC, session_id)
  WHERE error_count > 0;"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary_dirty (
  tenant_id    TEXT        NOT NULL,
  session_id   TEXT        NOT NULL,
  min_ts       TIMESTAMPTZ NOT NULL,
  max_ts       TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, session_id)
);"#
        ),
    ]
}

/// Ensure session summary tables exist in the tenant metadata schema.
pub async fn ensure_session_summary_tables(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    for ddl in session_summary_table_ddls(tenant_schema) {
        client.execute(&ddl, &[]).await?;
    }
    Ok(())
}

pub async fn ensure_shared_session_summary_tables(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    for ddl in shared_session_summary_table_ddls(tenant_schema) {
        client.execute(&ddl, &[]).await?;
    }
    Ok(())
}

/// Shared scopes may use only summary tables that carry workspace ownership
/// and composite logical keys. Fresh tables satisfy this automatically; older
/// isolated schemas must be migrated explicitly before shared mode is allowed.
pub async fn validate_shared_session_summary_tables(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> anyhow::Result<()> {
    for table_name in ["session_summary", "session_summary_dirty"] {
        let column_count: i64 = client
            .query_one(
                "SELECT count(*) FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 AND column_name = 'tenant_id';",
                &[&tenant_schema, &table_name],
            )
            .await?
            .get(0);
        if column_count != 1 {
            return Err(anyhow::anyhow!(
                "{}: {table_name} is missing tenant_id",
                crate::workspace_scope::SharedScopeError::new(
                    crate::workspace_scope::SharedScopeErrorCode::SchemaIncompatible,
                    format!("shared session-summary table {table_name} has no ownership column"),
                )
            ));
        }

        let primary_key_columns: i64 = client
            .query_one(
                "SELECT count(*) FROM information_schema.key_column_usage k \
                 JOIN information_schema.table_constraints c \
                   ON c.constraint_schema = k.constraint_schema \
                  AND c.constraint_name = k.constraint_name \
                  AND c.table_name = k.table_name \
                 WHERE k.table_schema = $1 AND k.table_name = $2 \
                   AND c.constraint_type = 'PRIMARY KEY' \
                   AND k.column_name IN ('tenant_id', 'session_id');",
                &[&tenant_schema, &table_name],
            )
            .await?
            .get(0);
        if primary_key_columns != 2 {
            return Err(anyhow::anyhow!(
                "{}: {table_name} must use (tenant_id, session_id) as its logical key",
                crate::workspace_scope::SharedScopeError::new(
                    crate::workspace_scope::SharedScopeErrorCode::SchemaIncompatible,
                    format!(
                        "shared session-summary table {table_name} has an incompatible primary key"
                    ),
                )
            ));
        }
    }
    Ok(())
}
