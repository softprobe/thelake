//! Per-tenant Postgres DDL for `session_summary` + `session_summary_dirty`.

use crate::runtime_engine::quote_pg_ident;
use anyhow::{Context, Result};

/// Fresh isolated-schema DDL. The SQL file is the source of truth.
pub fn session_summary_table_ddls(tenant_schema: &str) -> Vec<String> {
    summary_ddls(
        include_str!("../sql/schema/session_summary_isolated.sql"),
        tenant_schema,
    )
}

/// Fresh shared-schema DDL. The SQL file is the source of truth.
pub fn shared_session_summary_table_ddls(tenant_schema: &str) -> Vec<String> {
    summary_ddls(
        include_str!("../sql/schema/session_summary_shared.sql"),
        tenant_schema,
    )
}

fn summary_ddls(base: &str, tenant_schema: &str) -> Vec<String> {
    vec![
        render_summary_ddl(base, tenant_schema),
        render_summary_ddl(
            include_str!("../sql/schema/session_summary_dirty_function.sql"),
            tenant_schema,
        ),
        render_summary_ddl(
            include_str!("../sql/schema/session_summary_dirty_trigger.sql"),
            tenant_schema,
        ),
    ]
}

fn render_summary_ddl(template: &str, tenant_schema: &str) -> String {
    let quoted = quote_pg_ident(tenant_schema);
    let literal = format!("'{}'", tenant_schema.replace('\'', "''"));
    template
        .replace("{{schema}}", &quoted)
        .replace("{{schema_literal}}", &literal)
}

/// Ensure session summary tables exist in the tenant metadata schema.
pub async fn ensure_session_summary_tables(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<()> {
    for (index, ddl) in session_summary_table_ddls(tenant_schema)
        .into_iter()
        .enumerate()
    {
        client
            .batch_execute(&ddl)
            .await
            .with_context(|| format!("failed session summary DDL {index} in {tenant_schema}"))?;
    }
    Ok(())
}

pub async fn ensure_shared_session_summary_tables(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<()> {
    for (index, ddl) in shared_session_summary_table_ddls(tenant_schema)
        .into_iter()
        .enumerate()
    {
        client.batch_execute(&ddl).await.with_context(|| {
            format!("failed shared session summary DDL {index} in {tenant_schema}")
        })?;
    }
    Ok(())
}

/// Shared scopes may use only summary tables that carry workspace ownership
/// and composite logical keys. Fresh tables satisfy this automatically; older
/// an isolated schema must be recreated through the coordinated clean cutover.
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
