//! Per-tenant Postgres DDL for `session_summary` + `session_summary_dirty`.

use crate::runtime_engine::quote_pg_ident;

/// DDL statements for both summary tables (idempotent `IF NOT EXISTS`).
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
