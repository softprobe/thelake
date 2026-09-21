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

pub fn score_exists_sql(table: &str) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE score_id = ? \
         AND CAST(timestamp AS TIMESTAMP_NS) >= '1970-01-01'::TIMESTAMP_NS \
         AND CAST(timestamp AS TIMESTAMP_NS) <= '2100-01-01'::TIMESTAMP_NS LIMIT 1)"
    )
}

pub fn score_config_exists_sql(table: &str) -> String {
    format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE config_id = ? LIMIT 1)")
}

pub fn score_config_select_sql(table: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR) FROM {table} ORDER BY timestamp DESC, config_id DESC"
    )
}

pub fn score_config_by_id_sql(table: &str) -> String {
    format!(
        "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
         description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
         CAST(to_json(metadata) AS VARCHAR) FROM {table} WHERE config_id = ? LIMIT 1"
    )
}
