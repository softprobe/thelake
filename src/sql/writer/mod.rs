//! Writer-side SQL helpers (checked execute).

pub fn create_from_parquet_sql(table: &str, select: &str, path: &str) -> String {
    format!("CREATE TABLE IF NOT EXISTS {table} AS {select} FROM read_parquet('{path}') LIMIT 0;")
}

pub fn add_column_sql(table: &str, column: &str, duck_type: &str) -> String {
    format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {duck_type};")
}

pub fn insert_batch_sql(table: &str, select: &str, path: Option<&str>, order: &str) -> String {
    match path {
        Some(path) => {
            format!("INSERT INTO {table} BY NAME {select} FROM read_parquet('{path}') {order};")
        }
        None => format!("INSERT INTO {table} BY NAME\n{select};"),
    }
}

pub fn insert_deduped_parquet_sql(
    table: &str,
    select: &str,
    path: &str,
    id_column: &str,
    order: &str,
) -> String {
    format!(
        "INSERT INTO {table} BY NAME\n\
         SELECT incoming.* FROM (\n\
           {select} FROM read_parquet('{path}')\n\
         ) incoming\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} existing\n\
           WHERE existing.{id_column} = incoming.{id_column}\n\
         )\n\
         {order};"
    )
}

pub fn insert_deduped_parquet_sql_for_workspace(
    table: &str,
    select: &str,
    path: &str,
    id_column: &str,
    order: &str,
) -> String {
    format!(
        "INSERT INTO {table} BY NAME\n\
         SELECT incoming.* FROM (\n\
           {select} FROM read_parquet('{path}')\n\
         ) incoming\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} existing\n\
           WHERE existing.{id_column} = incoming.{id_column}\n\
             AND existing.tenant_id = incoming.tenant_id\n\
         )\n\
         {order};"
    )
}

// Scores use the microsecond/TIMESTAMPTZ family (see `ts_utc` in
// `storage::schema::tables`), not the timezone-free TIMESTAMP_NS trace/log
// clocks, so the bound compares against TIMESTAMPTZ directly.
pub fn score_exists_sql(table: &str) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE score_id = ? \
         AND timestamp >= '1970-01-01'::TIMESTAMPTZ \
         AND timestamp <= '2100-01-01'::TIMESTAMPTZ LIMIT 1)"
    )
}

pub fn score_exists_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE score_id = ? AND tenant_id = {} \
         AND timestamp >= '1970-01-01'::TIMESTAMPTZ \
         AND timestamp <= '2100-01-01'::TIMESTAMPTZ LIMIT 1)",
        crate::sql::sql_string_literal(workspace_id)
    )
}

pub fn score_config_exists_sql(table: &str) -> String {
    format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE config_id = ? LIMIT 1)")
}

pub fn score_config_exists_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE config_id = ? AND tenant_id = {} LIMIT 1)",
        crate::sql::sql_string_literal(workspace_id)
    )
}

pub fn score_config_select_sql(table: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), tenant_id::VARCHAR FROM {table} ORDER BY timestamp DESC, config_id DESC"
    )
}

pub fn score_config_select_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), tenant_id::VARCHAR FROM {table} \
         WHERE tenant_id = {} ORDER BY timestamp DESC, config_id DESC",
        crate::sql::sql_string_literal(workspace_id)
    )
}

pub fn score_config_by_id_sql(table: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), tenant_id::VARCHAR FROM {table} WHERE config_id = ? LIMIT 1"
    )
}

pub fn score_config_by_id_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), tenant_id::VARCHAR FROM {table} \
         WHERE config_id = ? AND tenant_id = {} LIMIT 1",
        crate::sql::sql_string_literal(workspace_id)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_score_queries_filter_and_escape_ownership() {
        let score = score_exists_sql_for_workspace("scores", "workspace'42");
        let config = score_config_by_id_sql_for_workspace("score_configs", "workspace'42");
        assert!(score.contains("tenant_id = 'workspace''42'"));
        assert!(config.contains("WHERE config_id = ? AND tenant_id = 'workspace''42'"));
    }

    #[test]
    fn score_config_projection_keeps_tenant_id_for_round_trip() {
        let sql = score_config_select_sql("score_configs");
        assert!(sql.contains("tenant_id::VARCHAR"));
    }

    #[test]
    fn shared_score_dedupe_uses_the_composite_logical_identity() {
        let sql = insert_deduped_parquet_sql_for_workspace(
            "scores",
            "SELECT *",
            "/tmp/scores.parquet",
            "score_id",
            "ORDER BY timestamp",
        );
        assert!(sql.contains("existing.score_id = incoming.score_id"));
        assert!(sql.contains("existing.tenant_id = incoming.tenant_id"));
    }
}
