//! Per-tenant Postgres DDL for `session_summary` + `session_summary_dirty`.

use crate::runtime_engine::quote_pg_ident;

fn quote_pg_literal(input: &str) -> String {
    format!("'{}'", input.replace('\'', "''"))
}

fn dirty_generation_trigger_ddls(tenant_schema: &str) -> [String; 2] {
    let schema = quote_pg_ident(tenant_schema);
    let schema_literal = quote_pg_literal(tenant_schema);
    [
        format!("CREATE OR REPLACE FUNCTION {schema}.session_summary_dirty_bump_generation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.generation := OLD.generation + 1; RETURN NEW; END; $$;"),
        format!("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'session_summary_dirty_bump_generation' AND tgrelid = to_regclass(format('%I.%I', {schema_literal}, 'session_summary_dirty')) AND NOT tgisinternal) THEN BEGIN CREATE TRIGGER session_summary_dirty_bump_generation BEFORE UPDATE ON {schema}.session_summary_dirty FOR EACH ROW EXECUTE FUNCTION {schema}.session_summary_dirty_bump_generation(); EXCEPTION WHEN duplicate_object THEN NULL; END; END IF; END $$;"),
    ]
}

fn summary_time_cutover_ddl(tenant_schema: &str) -> String {
    let schema = quote_pg_ident(tenant_schema);
    let schema_literal = quote_pg_literal(tenant_schema);
    format!(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = {schema_literal} AND table_name = 'session_summary' AND column_name = 'start_time') THEN \
         ALTER TABLE {schema}.session_summary ALTER COLUMN start_time TYPE BIGINT USING (EXTRACT(EPOCH FROM start_time) * 1000000000)::BIGINT; \
         ALTER TABLE {schema}.session_summary RENAME COLUMN start_time TO start_time_ns; \
         ALTER TABLE {schema}.session_summary ALTER COLUMN end_time TYPE BIGINT USING (EXTRACT(EPOCH FROM end_time) * 1000000000)::BIGINT; \
         ALTER TABLE {schema}.session_summary RENAME COLUMN end_time TO end_time_ns; \
         END IF; END $$;"
    )
}

fn dirty_time_cutover_ddl(tenant_schema: &str, shared: bool) -> String {
    let schema = quote_pg_ident(tenant_schema);
    let schema_literal = quote_pg_literal(tenant_schema);
    let insert_columns = if shared {
        "tenant_id, session_id, min_ts_ns, max_ts_ns, updated_at"
    } else {
        "session_id, min_ts_ns, max_ts_ns, updated_at"
    };
    let select_key = if shared {
        "s.tenant_id, s.session_id"
    } else {
        "s.session_id"
    };
    let on_conflict = if shared {
        "(tenant_id, session_id)"
    } else {
        "(session_id)"
    };
    format!(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = {schema_literal} AND table_name = 'session_summary_dirty' AND column_name = 'min_ts') THEN \
         ALTER TABLE {schema}.session_summary_dirty ALTER COLUMN min_ts TYPE BIGINT USING (EXTRACT(EPOCH FROM min_ts) * 1000000000)::BIGINT; \
         ALTER TABLE {schema}.session_summary_dirty RENAME COLUMN min_ts TO min_ts_ns; \
         ALTER TABLE {schema}.session_summary_dirty ALTER COLUMN max_ts TYPE BIGINT USING (EXTRACT(EPOCH FROM max_ts) * 1000000000)::BIGINT; \
         ALTER TABLE {schema}.session_summary_dirty RENAME COLUMN max_ts TO max_ts_ns; \
         UPDATE {schema}.session_summary_dirty SET min_ts_ns = min_ts_ns - 1000, max_ts_ns = max_ts_ns + 1000; \
         INSERT INTO {schema}.session_summary_dirty ({insert_columns}) \
         SELECT {select_key}, s.start_time_ns - 1000, COALESCE(s.end_time_ns, s.start_time_ns) + 1000, clock_timestamp() \
         FROM {schema}.session_summary s \
         ON CONFLICT {on_conflict} DO UPDATE SET min_ts_ns = LEAST({schema}.session_summary_dirty.min_ts_ns, EXCLUDED.min_ts_ns), \
         max_ts_ns = GREATEST({schema}.session_summary_dirty.max_ts_ns, EXCLUDED.max_ts_ns), updated_at = EXCLUDED.updated_at; \
         END IF; END $$;"
    )
}

/// Legacy per-workspace DDL for isolated scopes.
pub fn session_summary_table_ddls(tenant_schema: &str) -> Vec<String> {
    let schema = quote_pg_ident(tenant_schema);
    let mut ddls = vec![
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary (
  session_id          TEXT        NOT NULL,
  start_time_ns       BIGINT      NOT NULL,
  end_time_ns         BIGINT,
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
        summary_time_cutover_ddl(tenant_schema),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_recent
  ON {schema}.session_summary (start_time_ns DESC, session_id);"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_agent
  ON {schema}.session_summary (agent_name, start_time_ns DESC, session_id)
  WHERE agent_name IS NOT NULL;"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_errors
  ON {schema}.session_summary (start_time_ns DESC, session_id)
  WHERE error_count > 0;"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary_dirty (
  session_id   TEXT        NOT NULL,
  min_ts_ns    BIGINT      NOT NULL,
  max_ts_ns    BIGINT      NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  generation   BIGINT      NOT NULL DEFAULT 1,
  claim_holder TEXT,
  claim_until  TIMESTAMPTZ,
  PRIMARY KEY (session_id)
);"#
        ),
        format!("ALTER TABLE {schema}.session_summary_dirty ADD COLUMN IF NOT EXISTS claim_holder TEXT;"),
        format!("ALTER TABLE {schema}.session_summary_dirty ADD COLUMN IF NOT EXISTS claim_until TIMESTAMPTZ;"),
        format!("ALTER TABLE {schema}.session_summary_dirty ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;"),
        dirty_time_cutover_ddl(tenant_schema, false),
        format!("CREATE INDEX IF NOT EXISTS session_summary_dirty_claim ON {schema}.session_summary_dirty (claim_until) WHERE claim_holder IS NOT NULL;"),
    ]
    .into_iter()
    .collect::<Vec<_>>();
    ddls.splice(9..9, dirty_generation_trigger_ddls(tenant_schema));
    ddls
}

/// Composite-key DDL for a shared physical scope.
pub fn shared_session_summary_table_ddls(tenant_schema: &str) -> Vec<String> {
    let schema = quote_pg_ident(tenant_schema);
    let mut ddls = vec![
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary (
  tenant_id           TEXT        NOT NULL,
  session_id          TEXT        NOT NULL,
  start_time_ns       BIGINT      NOT NULL,
  end_time_ns         BIGINT,
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
        summary_time_cutover_ddl(tenant_schema),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_recent
  ON {schema}.session_summary (tenant_id, start_time_ns DESC, session_id);"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_agent
  ON {schema}.session_summary (tenant_id, agent_name, start_time_ns DESC, session_id)
  WHERE agent_name IS NOT NULL;"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS session_summary_errors
  ON {schema}.session_summary (tenant_id, start_time_ns DESC, session_id)
  WHERE error_count > 0;"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.session_summary_dirty (
  tenant_id    TEXT        NOT NULL,
  session_id   TEXT        NOT NULL,
  min_ts_ns    BIGINT      NOT NULL,
  max_ts_ns    BIGINT      NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  generation   BIGINT      NOT NULL DEFAULT 1,
  claim_holder TEXT,
  claim_until  TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, session_id)
);"#
        ),
        format!("ALTER TABLE {schema}.session_summary_dirty ADD COLUMN IF NOT EXISTS claim_holder TEXT;"),
        format!("ALTER TABLE {schema}.session_summary_dirty ADD COLUMN IF NOT EXISTS claim_until TIMESTAMPTZ;"),
        format!("ALTER TABLE {schema}.session_summary_dirty ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;"),
        dirty_time_cutover_ddl(tenant_schema, true),
        format!("CREATE INDEX IF NOT EXISTS session_summary_dirty_claim ON {schema}.session_summary_dirty (claim_until) WHERE claim_holder IS NOT NULL;"),
    ]
    .into_iter()
    .collect::<Vec<_>>();
    ddls.splice(9..9, dirty_generation_trigger_ddls(tenant_schema));
    ddls
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
