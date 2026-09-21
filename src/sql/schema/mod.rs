//! Authoritative production table registry and one-clock DDL.

/// Locked DuckLake partition expression (calendar day of `timestamp`).
/// Proven by `tests/integration/one_clock_prune.rs`.
pub const ONE_CLOCK_PARTITION_BY: &str = "year(timestamp), month(timestamp), day(timestamp)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFamily {
    Otlp,
    Auxiliary,
}

/// One authoritative production table definition.
#[derive(Debug, Clone, Copy)]
pub struct TableSpec {
    pub name: &'static str,
    /// Column definitions for registry-owned SQL DDL. OTLP tables are created
    /// from Arrow schemas, so their registry entry intentionally has none.
    pub columns_sql: &'static str,
    pub sorted_by: &'static str,
    pub family: TableFamily,
    pub is_fact: bool,
}

pub const TRACES: TableSpec = TableSpec {
    name: "traces",
    columns_sql: "",
    sorted_by: "session_id, trace_id, timestamp",
    family: TableFamily::Otlp,
    is_fact: true,
};

pub const LOGS: TableSpec = TableSpec {
    name: "logs",
    columns_sql: "",
    sorted_by: "session_id, timestamp",
    family: TableFamily::Otlp,
    is_fact: true,
};

pub const SCORES: TableSpec = TableSpec {
    name: "scores",
    columns_sql: "",
    sorted_by: "session_id, timestamp",
    family: TableFamily::Otlp,
    is_fact: true,
};

pub const SCORE_CONFIGS: TableSpec = TableSpec {
    name: "score_configs",
    columns_sql: "",
    sorted_by: "",
    family: TableFamily::Auxiliary,
    is_fact: false,
};

pub const OTLP_TABLES: &[TableSpec] = &[TRACES, LOGS, SCORES];

/// Every table whose production scan must carry a one-clock timestamp bound.
pub fn fact_table_specs() -> impl Iterator<Item = &'static TableSpec> {
    OTLP_TABLES.iter().filter(|table| table.is_fact)
}

pub fn table_spec(name: &str) -> Option<&'static TableSpec> {
    fact_table_specs()
        .chain(std::iter::once(&SCORE_CONFIGS))
        .find(|table| table.name == name)
}

pub fn is_otlp_table(name: &str) -> bool {
    OTLP_TABLES.iter().any(|table| table.name == name)
}

pub fn insert_order_by(name: &str) -> &'static str {
    table_spec(name)
        .filter(|table| is_otlp_table(table.name))
        .map(|table| match table.name {
            "traces" => "ORDER BY session_id, trace_id, timestamp",
            "logs" | "scores" => "ORDER BY session_id, timestamp",
            _ => "",
        })
        .unwrap_or("")
}

pub fn qualified_table_name(catalog: &str, table: &TableSpec) -> String {
    format!("{catalog}.{}", table.name)
}

pub fn create_table_sql(catalog: &str, table: &TableSpec) -> String {
    assert!(
        !table.columns_sql.is_empty(),
        "{} is created from its Arrow schema, not registry SQL DDL",
        table.name
    );
    let qualified = qualified_table_name(catalog, table);
    format!(
        "CREATE TABLE IF NOT EXISTS {qualified} (\n  {}\n);",
        table.columns_sql.replace(", ", ",\n  ")
    )
}

pub fn partition_sort_sql(catalog: &str, table: &TableSpec) -> String {
    let qualified = qualified_table_name(catalog, table);
    format!(
        "ALTER TABLE {qualified} SET PARTITIONED BY ({ONE_CLOCK_PARTITION_BY});\n\
         ALTER TABLE {qualified} SET SORTED BY ({});",
        table.sorted_by
    )
}

pub fn ensure_table_sql(catalog: &str, table: &TableSpec) -> String {
    format!(
        "{}\n{}",
        create_table_sql(catalog, table),
        partition_sort_sql(catalog, table)
    )
}

pub fn add_column_sql(table: &str, column: &str, sql_type: &str) -> String {
    format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {sql_type};")
}

pub fn alter_column_type_sql(table: &str, column: &str, sql_type: &str) -> String {
    format!("ALTER TABLE {table} ALTER COLUMN {column} SET DATA TYPE {sql_type};")
}

pub fn alter_column_using_sql(table: &str, column: &str, sql_type: &str, using: &str) -> String {
    format!("ALTER TABLE {table} ALTER COLUMN {column} SET DATA TYPE {sql_type} USING {using};")
}

impl TableSpec {
    pub fn qualified(&self, catalog: &str) -> String {
        qualified_table_name(catalog, self)
    }

    pub fn partition_sort_sql(&self, catalog: &str) -> String {
        partition_sort_sql(catalog, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_uses_year_month_day_of_timestamp() {
        let sql = TRACES.partition_sort_sql("softprobe");
        assert!(sql.contains(ONE_CLOCK_PARTITION_BY));
        assert!(!sql.contains("record_date"));
    }

    #[test]
    fn registry_covers_all_production_fact_tables() {
        let mut names: Vec<_> = fact_table_specs().map(|table| table.name).collect();
        names.sort_unstable();
        let mut expected = vec!["traces", "logs", "scores"];
        expected.sort_unstable();
        assert_eq!(names, expected);
    }

    #[test]
    fn every_registry_fact_has_timestamp_ddl_or_otlp_schema() {
        for table in fact_table_specs() {
            assert!(
                !table.sorted_by.is_empty(),
                "{} has no sort key",
                table.name
            );
            if !is_otlp_table(table.name) {
                assert!(table.columns_sql.contains("timestamp"), "{}", table.name);
            }
        }
    }

    #[test]
    fn registry_names_match_otlp_arrow_schema_owners() {
        assert_eq!(
            TRACES.name,
            crate::storage::schema::TraceTable::table_name()
        );
        assert_eq!(
            LOGS.name,
            crate::storage::schema::OtlpLogsTable::table_name()
        );
        assert_eq!(
            SCORES.name,
            crate::storage::schema::ScoreTable::table_name()
        );
        assert_eq!(
            SCORE_CONFIGS.name,
            crate::storage::schema::ScoreConfigTable::table_name()
        );
        for table in OTLP_TABLES {
            assert!(!table.sorted_by.is_empty(), "{}", table.name);
        }
    }

    #[test]
    fn fact_table_specs_contain_no_metric_tables() {
        for table in fact_table_specs() {
            assert!(
                !table.name.starts_with("metric_"),
                "unexpected metrics fact table {}",
                table.name
            );
            assert_ne!(table.name, "metrics");
        }
    }
}
