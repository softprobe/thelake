//! Writer-side SQL helpers (checked execute).

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
    timestamp_window: Option<&crate::sql::QueryWindow>,
) -> String {
    insert_deduped_parquet_sql_inner(
        table,
        select,
        path,
        id_column,
        order,
        false,
        timestamp_window,
    )
}

pub fn insert_deduped_parquet_sql_for_workspace(
    table: &str,
    select: &str,
    path: &str,
    id_column: &str,
    order: &str,
    timestamp_window: Option<&crate::sql::QueryWindow>,
) -> String {
    insert_deduped_parquet_sql_inner(
        table,
        select,
        path,
        id_column,
        order,
        true,
        timestamp_window,
    )
}

fn insert_deduped_parquet_sql_inner(
    table: &str,
    select: &str,
    path: &str,
    id_column: &str,
    order: &str,
    shared_scope: bool,
    timestamp_window: Option<&crate::sql::QueryWindow>,
) -> String {
    let tenant_predicate = if shared_scope {
        " AND existing.workspace_id = incoming.workspace_id"
    } else {
        ""
    };
    let timestamp_filter = timestamp_window.map_or_else(String::new, |window| {
        format!(" AND {}", window.timestamp_filter_sql("existing."))
    });
    format!(
        "INSERT INTO {table} BY NAME\n\
         SELECT incoming.* FROM (\n\
           {select} FROM read_parquet('{path}')\n\
         ) incoming\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} existing\n\
           WHERE existing.{id_column} = incoming.{id_column}{tenant_predicate}{timestamp_filter}\n\
         )\n\
         {order};"
    )
}

pub fn score_exists_sql(table: &str, window: &crate::sql::QueryWindow) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE score_id = ? AND {} LIMIT 1)",
        window.timestamp_filter_sql("")
    )
}

pub fn score_exists_sql_for_workspace(
    table: &str,
    workspace_id: &str,
    window: &crate::sql::QueryWindow,
) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE score_id = ? AND workspace_id = {} AND {} LIMIT 1)",
        crate::sql::sql_string_literal(workspace_id),
        window.timestamp_filter_sql("")
    )
}

pub fn score_config_exists_sql(table: &str) -> String {
    format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE config_id = ? LIMIT 1)")
}

pub fn score_config_exists_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE config_id = ? AND workspace_id = {} LIMIT 1)",
        crate::sql::sql_string_literal(workspace_id)
    )
}

pub fn score_config_select_sql(table: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, timestamp::VARCHAR, name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), workspace_id::VARCHAR FROM {table} ORDER BY timestamp DESC, config_id DESC"
    )
}

pub fn score_config_select_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, timestamp::VARCHAR, name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), workspace_id::VARCHAR FROM {table} \
         WHERE workspace_id = {} ORDER BY timestamp DESC, config_id DESC",
        crate::sql::sql_string_literal(workspace_id)
    )
}

pub fn score_config_by_id_sql(table: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), workspace_id::VARCHAR FROM {table} WHERE config_id = ? LIMIT 1"
    )
}

pub fn score_config_by_id_sql_for_workspace(table: &str, workspace_id: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR), workspace_id::VARCHAR FROM {table} \
         WHERE config_id = ? AND workspace_id = {} LIMIT 1",
        crate::sql::sql_string_literal(workspace_id)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn score_window() -> crate::sql::QueryWindow {
        let timestamp = Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap();
        crate::sql::QueryWindow::try_new(timestamp, timestamp).unwrap()
    }

    #[test]
    fn workspace_score_queries_filter_and_escape_ownership() {
        let score = score_exists_sql_for_workspace("scores", "workspace'42", &score_window());
        let config = score_config_by_id_sql_for_workspace("score_configs", "workspace'42");
        assert!(score.contains("workspace_id = 'workspace''42'"));
        assert!(score.contains("timestamp >= '2026-09-10T12:00:00.000000000Z'::TIMESTAMP_NS"));
        assert!(score.contains("timestamp <= '2026-09-10T12:00:00.000000000Z'::TIMESTAMP_NS"));
        assert!(config.contains("WHERE config_id = ? AND workspace_id = 'workspace''42'"));
    }

    #[test]
    fn score_config_projection_keeps_tenant_id_for_round_trip() {
        let sql = score_config_select_sql("score_configs");
        assert!(sql.contains("workspace_id::VARCHAR"));
    }

    #[test]
    fn shared_score_dedupe_uses_the_composite_logical_identity() {
        let sql = insert_deduped_parquet_sql_for_workspace(
            "scores",
            "SELECT *",
            "/tmp/scores.parquet",
            "score_id",
            "ORDER BY timestamp",
            Some(&score_window()),
        );
        assert!(sql.contains("existing.score_id = incoming.score_id"));
        assert!(sql.contains("existing.workspace_id = incoming.workspace_id"));
        assert!(
            sql.contains("existing.timestamp >= '2026-09-10T12:00:00.000000000Z'::TIMESTAMP_NS")
        );
        assert!(
            sql.contains("existing.timestamp <= '2026-09-10T12:00:00.000000000Z'::TIMESTAMP_NS")
        );
    }
}
