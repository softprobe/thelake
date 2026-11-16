const PG_TABLE_COLUMNS_SQL: &str = include_str!("migrate_variant/pg_table_columns.sql");
const PG_PHYSICAL_SCOPE_SQL: &str = include_str!("migrate_variant/pg_physical_scope.sql");
const PG_WORKSPACE_COUNT_SQL: &str = include_str!("migrate_variant/pg_workspace_count.sql");
const PG_COPY_WORKSPACES_SQL: &str = include_str!("migrate_variant/pg_copy_workspaces.sql");
const PG_MISSING_WORKSPACES_SQL: &str = include_str!("migrate_variant/pg_missing_workspaces.sql");
const TABLE_COUNT_SQL: &str = include_str!("migrate_variant/table_count.sql");
const PG_COPY_TABLE_SQL: &str = include_str!("migrate_variant/pg_copy_table.sql");
const DUCKLAKE_INIT_SQL: &str = include_str!("migrate_variant/ducklake_init.sql");
const DUCKLAKE_ATTACH_SQL: &str = include_str!("migrate_variant/ducklake_attach.sql");
const DUCKLAKE_TABLE_EXISTS_SQL: &str = include_str!("migrate_variant/ducklake_table_exists.sql");
const DUCKLAKE_DISTINCT_DAYS_SQL: &str = include_str!("migrate_variant/ducklake_distinct_days.sql");
const DUCKLAKE_DAY_COUNT_SQL: &str = include_str!("migrate_variant/ducklake_day_count.sql");
const DUCKLAKE_INSERT_DAY_SQL: &str = include_str!("migrate_variant/ducklake_insert_day.sql");
const DUCKLAKE_INSERT_ALL_SQL: &str = include_str!("migrate_variant/ducklake_insert_all.sql");
const DUCKLAKE_TRACES_DEDUP_SQL: &str = include_str!("migrate_variant/ducklake_traces_dedup.sql");

pub(crate) fn pg_table_columns_sql() -> &'static str {
    PG_TABLE_COLUMNS_SQL
}

pub(crate) fn render_pg_physical_scope_sql(dst_schema: &str) -> String {
    PG_PHYSICAL_SCOPE_SQL.replace("{{dst_schema}}", dst_schema)
}

pub(crate) fn render_pg_workspace_count_sql(schema: &str) -> String {
    PG_WORKSPACE_COUNT_SQL.replace("{{schema}}", schema)
}

pub(crate) fn render_pg_copy_workspaces_sql(
    src_schema: &str,
    dst_schema: &str,
    insert_list: &str,
    select_list: &str,
) -> String {
    PG_COPY_WORKSPACES_SQL
        .replace("{{src_schema}}", src_schema)
        .replace("{{dst_schema}}", dst_schema)
        .replace("{{insert_list}}", insert_list)
        .replace("{{select_list}}", select_list)
}

pub(crate) fn render_pg_missing_workspaces_sql(src_schema: &str, dst_schema: &str) -> String {
    PG_MISSING_WORKSPACES_SQL
        .replace("{{src_schema}}", src_schema)
        .replace("{{dst_schema}}", dst_schema)
}

pub(crate) fn render_table_count_sql(schema: &str, table: &str) -> String {
    TABLE_COUNT_SQL
        .replace("{{schema}}", schema)
        .replace("{{table}}", table)
}

pub(crate) fn render_pg_copy_table_sql(
    src_schema: &str,
    dst_schema: &str,
    table: &str,
    insert_list: &str,
    select_list: &str,
) -> String {
    PG_COPY_TABLE_SQL
        .replace("{{src_schema}}", src_schema)
        .replace("{{dst_schema}}", dst_schema)
        .replace("{{table}}", table)
        .replace("{{insert_list}}", insert_list)
        .replace("{{select_list}}", select_list)
}

pub(crate) fn render_ducklake_init_sql(ext_path: &str) -> String {
    DUCKLAKE_INIT_SQL.replace("{{ext_path}}", ext_path)
}

pub(crate) fn ducklake_table_exists_sql() -> &'static str {
    DUCKLAKE_TABLE_EXISTS_SQL
}

pub(crate) fn render_ducklake_attach_sql(
    dsn: &str,
    src_schema: &str,
    src_data_path: &str,
    dst_schema: &str,
    dst_data_path: &str,
) -> String {
    DUCKLAKE_ATTACH_SQL
        .replace("{{dsn}}", dsn)
        .replace("{{src_schema}}", src_schema)
        .replace("{{src_data_path}}", src_data_path)
        .replace("{{dst_schema}}", dst_schema)
        .replace("{{dst_data_path}}", dst_data_path)
}

pub(crate) fn render_ducklake_distinct_days_sql(table: &str) -> String {
    DUCKLAKE_DISTINCT_DAYS_SQL.replace("{{table}}", table)
}

pub(crate) fn render_ducklake_day_count_sql(table: &str, day_start: &str) -> String {
    DUCKLAKE_DAY_COUNT_SQL
        .replace("{{table}}", table)
        .replace("{{day_start}}", day_start)
}

pub(crate) fn render_ducklake_insert_day_sql(
    table: &str,
    insert_cols: &str,
    select_exprs: &str,
    day_start: &str,
    dedup_traces: bool,
) -> String {
    let dedup_clause = if dedup_traces {
        format!(
            " {}",
            DUCKLAKE_TRACES_DEDUP_SQL.replace("{{day_start}}", day_start)
        )
    } else {
        String::new()
    };
    DUCKLAKE_INSERT_DAY_SQL
        .replace("{{table}}", table)
        .replace("{{insert_cols}}", insert_cols)
        .replace("{{select_exprs}}", select_exprs)
        .replace("{{day_start}}", day_start)
        .replace("{{dedup_clause}}", &dedup_clause)
}

pub(crate) fn render_ducklake_insert_all_sql(
    table: &str,
    insert_cols: &str,
    select_exprs: &str,
) -> String {
    DUCKLAKE_INSERT_ALL_SQL
        .replace("{{table}}", table)
        .replace("{{insert_cols}}", insert_cols)
        .replace("{{select_exprs}}", select_exprs)
}
