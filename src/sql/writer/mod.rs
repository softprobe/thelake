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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn score_window() -> crate::sql::QueryWindow {
        let timestamp = Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap();
        crate::sql::QueryWindow::try_new(timestamp, timestamp).unwrap()
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
