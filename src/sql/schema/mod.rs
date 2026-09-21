//! Authoritative production table registry and one-clock DDL.

/// Locked DuckLake partition expression (calendar day of `timestamp`).
/// Proven by `tests/integration/one_clock_prune.rs`.
pub const ONE_CLOCK_PARTITION_BY: &str = "year(timestamp), month(timestamp), day(timestamp)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFamily {
    Otlp,
    MetricsCore,
    MetricsDownsample,
    MetricsCollapse,
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

pub const METRIC_SAMPLES: TableSpec = TableSpec {
    name: "metric_samples",
    // Metrics use the timezone-bearing query clock; OTLP traces/logs use
    // TIMESTAMP_NS in their Arrow schemas for nanosecond API fidelity.
    columns_sql: "series_id UBIGINT, timestamp TIMESTAMPTZ, value DOUBLE",
    sorted_by: "series_id, timestamp",
    family: TableFamily::MetricsCore,
    is_fact: true,
};

const METRIC_SERIES: TableSpec = TableSpec {
    name: "metric_series",
    columns_sql: "series_id UBIGINT, metric_name VARCHAR, metric_type VARCHAR, unit VARCHAR, description VARCHAR, aggregation_temporality VARCHAR, is_monotonic BOOLEAN, labels MAP(VARCHAR, VARCHAR), timestamp TIMESTAMPTZ",
    sorted_by: "metric_name, series_id, timestamp",
    family: TableFamily::MetricsCore,
    is_fact: true,
};

const METRIC_POSTINGS: TableSpec = TableSpec {
    name: "metric_postings",
    columns_sql:
        "label_name VARCHAR, label_value VARCHAR, series_id UBIGINT, timestamp TIMESTAMPTZ",
    sorted_by: "label_name, label_value, series_id, timestamp",
    family: TableFamily::MetricsCore,
    is_fact: true,
};

const METRIC_HIST_SAMPLES: TableSpec = TableSpec {
    name: "metric_hist_samples",
    columns_sql: "series_id UBIGINT, timestamp TIMESTAMPTZ, count UBIGINT, sum DOUBLE, bucket_counts UBIGINT[], explicit_bounds DOUBLE[], quantiles VARCHAR, exemplars_json VARCHAR",
    sorted_by: "series_id, timestamp",
    family: TableFamily::MetricsCore,
    is_fact: true,
};

const DOWNSAMPLE_COLUMNS_SQL: &str = "series_id UBIGINT, timestamp TIMESTAMPTZ, count UBIGINT, sum DOUBLE, min DOUBLE, max DOUBLE, last DOUBLE, last_ts TIMESTAMPTZ";
const HIST_DOWNSAMPLE_COLUMNS_SQL: &str = "series_id UBIGINT, timestamp TIMESTAMPTZ, count UBIGINT, sum DOUBLE, bucket_counts UBIGINT[], explicit_bounds DOUBLE[], last_ts TIMESTAMPTZ";

const METRIC_SAMPLES_5M: TableSpec = TableSpec {
    name: "metric_samples_5m",
    columns_sql: DOWNSAMPLE_COLUMNS_SQL,
    sorted_by: "series_id, timestamp",
    family: TableFamily::MetricsDownsample,
    is_fact: true,
};
const METRIC_SAMPLES_1H: TableSpec = TableSpec {
    name: "metric_samples_1h",
    columns_sql: DOWNSAMPLE_COLUMNS_SQL,
    sorted_by: "series_id, timestamp",
    family: TableFamily::MetricsDownsample,
    is_fact: true,
};
const METRIC_HIST_SAMPLES_5M: TableSpec = TableSpec {
    name: "metric_hist_samples_5m",
    columns_sql: HIST_DOWNSAMPLE_COLUMNS_SQL,
    sorted_by: "series_id, timestamp",
    family: TableFamily::MetricsDownsample,
    is_fact: true,
};
const METRIC_HIST_SAMPLES_1H: TableSpec = TableSpec {
    name: "metric_hist_samples_1h",
    columns_sql: HIST_DOWNSAMPLE_COLUMNS_SQL,
    sorted_by: "series_id, timestamp",
    family: TableFamily::MetricsDownsample,
    is_fact: true,
};

const METRIC_COLLAPSE_JOB_1H: TableSpec = TableSpec {
    name: "metric_collapse_job_1h",
    columns_sql: "metric_name VARCHAR, job VARCHAR, timestamp TIMESTAMPTZ, count UBIGINT, sum DOUBLE, min DOUBLE, max DOUBLE, last DOUBLE",
    sorted_by: "metric_name, job, timestamp",
    family: TableFamily::MetricsCollapse,
    is_fact: true,
};

pub const OTLP_TABLES: &[TableSpec] = &[TRACES, LOGS, SCORES];
pub const METRICS_LAYOUT_CORE_TABLES: &[TableSpec] = &[
    METRIC_SERIES,
    METRIC_POSTINGS,
    METRIC_SAMPLES,
    METRIC_HIST_SAMPLES,
];
pub const METRICS_LAYOUT_DOWNSAMPLE_TABLES: &[TableSpec] = &[
    METRIC_SAMPLES_5M,
    METRIC_SAMPLES_1H,
    METRIC_HIST_SAMPLES_5M,
    METRIC_HIST_SAMPLES_1H,
];
pub const METRICS_LAYOUT_COLLAPSE_TABLES: &[TableSpec] = &[METRIC_COLLAPSE_JOB_1H];
pub const METRICS_MAINTENANCE_TABLES: &[TableSpec] = &[
    METRIC_SAMPLES,
    METRIC_POSTINGS,
    METRIC_SERIES,
    METRIC_HIST_SAMPLES,
    METRIC_SAMPLES_5M,
    METRIC_SAMPLES_1H,
    METRIC_HIST_SAMPLES_5M,
    METRIC_HIST_SAMPLES_1H,
    METRIC_COLLAPSE_JOB_1H,
];

pub fn metrics_layout_table_names() -> Vec<&'static str> {
    METRICS_MAINTENANCE_TABLES
        .iter()
        .map(|table| table.name)
        .collect()
}

/// Every table whose production scan must carry a one-clock timestamp bound.
pub fn fact_table_specs() -> impl Iterator<Item = &'static TableSpec> {
    METRICS_LAYOUT_CORE_TABLES
        .iter()
        .chain(METRICS_LAYOUT_DOWNSAMPLE_TABLES)
        .chain(METRICS_LAYOUT_COLLAPSE_TABLES)
        .chain(OTLP_TABLES)
        .filter(|table| table.is_fact)
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

pub fn union_metrics_sql(catalog_prefix: &str) -> String {
    let series = qualified_table_name(catalog_prefix, &METRIC_SERIES);
    let samples = qualified_table_name(catalog_prefix, &METRIC_SAMPLES);
    let hist = qualified_table_name(catalog_prefix, &METRIC_HIST_SAMPLES);
    let sample_series_day = crate::sql::same_utc_calendar_day("sm.timestamp", "s.timestamp");
    let hist_series_day = crate::sql::same_utc_calendar_day("h.timestamp", "s.timestamp");
    format!(
        "SELECT s.metric_name, s.description, s.unit, s.metric_type, sm.timestamp, sm.value, \
s.labels AS attributes, s.labels AS resource_attributes, NULL::UBIGINT AS count, \
NULL::DOUBLE AS sum, NULL::UBIGINT[] AS bucket_counts, NULL::DOUBLE[] AS explicit_bounds, \
NULL::VARCHAR AS quantiles, s.aggregation_temporality, NULL::VARCHAR AS exemplars_json \
FROM {samples} sm JOIN {series} s ON sm.series_id = s.series_id AND {sample_series_day} \
UNION ALL SELECT s.metric_name, s.description, s.unit, s.metric_type, h.timestamp, \
COALESCE(h.sum, 0.0) AS value, s.labels AS attributes, s.labels AS resource_attributes, \
h.count, h.sum, h.bucket_counts, h.explicit_bounds, h.quantiles, \
s.aggregation_temporality, h.exemplars_json FROM {hist} h JOIN {series} s ON h.series_id = s.series_id AND {hist_series_day}"
    )
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
        let mut expected = vec![
            "metric_samples",
            "metric_postings",
            "metric_series",
            "metric_hist_samples",
            "metric_samples_5m",
            "metric_samples_1h",
            "metric_hist_samples_5m",
            "metric_hist_samples_1h",
            "metric_collapse_job_1h",
            "traces",
            "logs",
            "scores",
        ];
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
    fn registry_ddl_and_partition_metadata_use_the_same_specs() {
        for table in METRICS_LAYOUT_CORE_TABLES
            .iter()
            .chain(METRICS_LAYOUT_DOWNSAMPLE_TABLES)
            .chain(METRICS_LAYOUT_COLLAPSE_TABLES)
        {
            let ddl = ensure_table_sql("softprobe", table);
            assert!(ddl.contains(table.name), "{}: {ddl}", table.name);
            assert!(ddl.contains(table.sorted_by), "{}: {ddl}", table.name);
            assert!(
                ddl.contains(ONE_CLOCK_PARTITION_BY),
                "{}: {ddl}",
                table.name
            );
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
}
