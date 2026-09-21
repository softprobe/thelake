//! Writer-side SQL helpers (checked execute).

pub fn insert_series_sql(table: &str, values: &str) -> String {
    // `series_id` is the stable metric identity, but this table is a day-scoped
    // index fact. Keep one representative row per UTC calendar day so Prom
    // day-window queries can resolve a persistent series without creating one
    // index row for every sample timestamp.
    format!(
        "INSERT INTO {table} (series_id, metric_name, metric_type, unit, description, aggregation_temporality, is_monotonic, labels, timestamp)\n\
         SELECT * FROM (VALUES\n{values}\n) AS v(series_id, metric_name, metric_type, unit, description, aggregation_temporality, is_monotonic, labels, timestamp)\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} e\n\
           WHERE e.series_id = v.series_id\n\
             AND {day_match}\n\
         );"
        ,
        day_match = crate::sql::same_utc_calendar_day(
            "e.timestamp",
            "CAST(v.timestamp AS TIMESTAMPTZ)"
        )
    )
}

pub fn insert_postings_sql(table: &str, values: &str) -> String {
    // Postings use the same day-scoped identity as metric_series. Exact sample
    // timestamps would multiply the inverted index at scrape frequency, while
    // omitting the day makes later-day Prom resolution miss persistent series.
    format!(
        "INSERT INTO {table} (label_name, label_value, series_id, timestamp)\n\
         SELECT * FROM (VALUES\n{values}\n) AS v(label_name, label_value, series_id, timestamp)\n\
         WHERE NOT EXISTS (\n\
           SELECT 1 FROM {table} e\n\
           WHERE e.label_name = v.label_name\n\
             AND e.label_value = v.label_value\n\
             AND e.series_id = v.series_id\n\
             AND {day_match}\n\
         );",
        day_match =
            crate::sql::same_utc_calendar_day("e.timestamp", "CAST(v.timestamp AS TIMESTAMPTZ)")
    )
}

pub fn insert_samples_sql(table: &str, values: &str) -> String {
    format!("INSERT INTO {table} (series_id, timestamp, value) VALUES\n{values};")
}

pub fn insert_hist_sql(table: &str, values: &str) -> String {
    format!(
        "INSERT INTO {table} (series_id, timestamp, count, sum, bucket_counts, explicit_bounds, quantiles, exemplars_json)\n\
         VALUES\n{values};"
    )
}

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
