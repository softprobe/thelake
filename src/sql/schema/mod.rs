//! Authoritative production table registry and one-clock DDL.

pub const TRACES_DDL: &str = include_str!("traces.sql");
pub const LOGS_DDL: &str = include_str!("logs.sql");
pub const SCORES_DDL: &str = include_str!("scores.sql");
pub const SCORE_CONFIGS_DDL: &str = include_str!("score_configs.sql");
pub const OTLP_LAYOUT_SQL: &str = include_str!("otlp_layout.sql");

fn layout_profile_values() -> &'static std::collections::HashMap<&'static str, String> {
    static VALUES: std::sync::OnceLock<std::collections::HashMap<&'static str, String>> =
        std::sync::OnceLock::new();
    VALUES.get_or_init(|| {
        OTLP_LAYOUT_SQL
            .lines()
            .filter_map(|line| {
                let rest = line.trim().strip_prefix("SET VARIABLE ")?;
                let (name, value) = rest.split_once(" = ")?;
                let value = value.trim().trim_end_matches(';').trim();
                let value = value.strip_prefix('\'')?.strip_suffix('\'')?;
                Some((name, value.to_string()))
            })
            .collect()
    })
}

fn layout_profile_value(name: &'static str) -> &'static str {
    layout_profile_values()
        .get(name)
        .unwrap_or_else(|| panic!("missing OTLP setting {name} in otlp_layout.sql"))
}

pub fn base_table_ddl(table: &str) -> Option<&'static str> {
    match table {
        "traces" => Some(TRACES_DDL),
        "logs" => Some(LOGS_DDL),
        "scores" => Some(SCORES_DDL),
        "score_configs" => Some(SCORE_CONFIGS_DDL),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFamily {
    Otlp,
    Auxiliary,
}

/// One authoritative production table definition.
#[derive(Debug, Clone, Copy)]
pub struct TableSpec {
    pub name: &'static str,
    /// Base table DDL is maintained in `src/sql/schema/*.sql`.
    pub family: TableFamily,
    pub is_fact: bool,
}

pub const TRACES: TableSpec = TableSpec {
    name: "traces",
    family: TableFamily::Otlp,
    is_fact: true,
};

pub const LOGS: TableSpec = TableSpec {
    name: "logs",
    family: TableFamily::Otlp,
    is_fact: true,
};

pub const SCORES: TableSpec = TableSpec {
    name: "scores",
    family: TableFamily::Otlp,
    is_fact: true,
};

pub const SCORE_CONFIGS: TableSpec = TableSpec {
    name: "score_configs",
    family: TableFamily::Auxiliary,
    is_fact: false,
};

pub const OTLP_TABLES: &[TableSpec] = &[TRACES, LOGS, SCORES];

/// Every fact table whose production scan requires a timestamp predicate.
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
    if table_spec(name).is_some_and(|table| is_otlp_table(table.name)) {
        // The SQL profile is also consumed by the Python exporter.
        static ORDER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        ORDER
            .get_or_init(|| {
                format!(
                    "ORDER BY {}",
                    layout_profile_value("thelake_otlp_sorted_by")
                )
            })
            .as_str()
    } else {
        ""
    }
}

pub fn qualified_table_name(catalog: &str, table: &TableSpec) -> String {
    format!("{catalog}.{}", table.name)
}

pub fn create_table_sql(catalog: &str, table: &TableSpec) -> String {
    let source = base_table_ddl(table.name).expect("persisted table must have SQL DDL");
    let qualified = qualified_table_name(catalog, table);
    source.replacen(
        &format!("CREATE TABLE IF NOT EXISTS {}", table.name),
        &format!("CREATE TABLE IF NOT EXISTS {qualified}"),
        1,
    )
}

pub fn partition_sort_sql(catalog: &str, table: &TableSpec) -> String {
    let qualified = qualified_table_name(catalog, table);
    let partition = layout_profile_value("thelake_otlp_partition_by");
    let sorted_by = layout_profile_value("thelake_otlp_sorted_by");
    format!(
        "ALTER TABLE {qualified} SET PARTITIONED BY ({partition});\n\
         ALTER TABLE {qualified} SET SORTED BY ({sorted_by});"
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
        assert!(sql.contains(layout_profile_value("thelake_otlp_partition_by")));
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
                insert_order_by(table.name).starts_with("ORDER BY "),
                "{} has no sort key from profile",
                table.name
            );
            assert!(base_table_ddl(table.name).is_some(), "{}", table.name);
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
            assert!(
                insert_order_by(table.name).starts_with("ORDER BY "),
                "{}",
                table.name
            );
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

    #[test]
    fn persisted_tables_have_canonical_sql_ddl_sources() {
        for (table, ddl) in [
            ("traces", TRACES_DDL),
            ("logs", LOGS_DDL),
            ("scores", SCORES_DDL),
            ("score_configs", SCORE_CONFIGS_DDL),
        ] {
            assert!(
                ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")),
                "{table}"
            );
            assert!(ddl.contains("timestamp"), "{table}");
        }
    }
}
