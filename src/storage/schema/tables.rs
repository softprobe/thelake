use crate::promotion::{PromotionColumn, PromotionDataType};
use crate::sql::schema::{base_table_ddl, LOGS, SCORES, SCORE_CONFIGS, TRACES};
use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::sync::Arc;

fn utf8() -> DataType {
    DataType::Utf8
}

fn timestamp_ns() -> DataType {
    // DuckLake event timestamps are UTC instants stored as timezone-free nanoseconds.
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

pub(crate) fn base_schema(table: &str) -> Schema {
    let ddl = base_table_ddl(table).unwrap_or_else(|| panic!("missing SQL schema for {table}"));
    let conn = duckdb::Connection::open_in_memory().expect("open DuckDB for SQL schema");
    conn.execute_batch(ddl)
        .expect("execute canonical table DDL");
    let mut stmt = conn
        .prepare(&format!("DESCRIBE {table}"))
        .expect("prepare canonical schema description");
    let mut rows = stmt.query([]).expect("describe canonical schema");
    let mut fields = Vec::new();
    while let Some(row) = rows.next().expect("read canonical column") {
        let name: String = row.get(0).expect("column name");
        let duck_type: String = row.get(1).expect("column type");
        let nullable: String = row.get(2).expect("column nullability");
        let data_type = match duck_type.to_ascii_uppercase().as_str() {
            "VARCHAR" | "TEXT" => DataType::Utf8,
            "BOOLEAN" => DataType::Boolean,
            "TINYINT" | "SMALLINT" | "INTEGER" | "INT" => DataType::Int32,
            "BIGINT" => DataType::Int64,
            "FLOAT" | "REAL" | "DOUBLE" => DataType::Float64,
            "TIMESTAMP_NS" | "TIMESTAMP_NANOSECONDS" => DataType::Timestamp(Nanosecond, None),
            "MAP(VARCHAR, VARCHAR)" => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            other => panic!("unsupported canonical SQL DuckDB type {other} in {table}"),
        };
        fields.push(Field::new(
            &name,
            data_type,
            nullable.eq_ignore_ascii_case("YES"),
        ));
    }
    Schema::new(fields)
}

fn trace_base_schema() -> &'static Schema {
    static SCHEMA: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| base_schema(TRACES.name))
}

fn logs_base_schema() -> &'static Schema {
    static SCHEMA: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| base_schema(LOGS.name))
}

fn scores_base_schema() -> &'static Schema {
    static SCHEMA: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| base_schema(SCORES.name))
}

fn score_configs_base_schema() -> &'static Schema {
    static SCHEMA: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| base_schema(SCORE_CONFIGS.name))
}

fn promoted_fields(base: &[Field], columns: &[PromotionColumn]) -> Vec<Field> {
    let existing: std::collections::HashSet<String> =
        base.iter().map(|f| f.name().to_ascii_lowercase()).collect();
    columns
        .iter()
        .filter(|column| !existing.contains(&column.name.to_ascii_lowercase()))
        .map(|column| {
            let data_type = match column.data_type {
                PromotionDataType::String | PromotionDataType::Json => utf8(),
                PromotionDataType::Bool => DataType::Boolean,
                PromotionDataType::Int64 => DataType::Int64,
                PromotionDataType::Double | PromotionDataType::Decimal => DataType::Float64,
                PromotionDataType::Timestamp => timestamp_ns(),
            };
            Field::new(&column.name, data_type, true)
        })
        .collect()
}

/// Raw sessions table - stores OTLP spans
pub struct TraceTable;

impl TraceTable {
    pub fn table_name() -> &'static str {
        TRACES.name
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let mut fields = trace_base_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.extend(promoted_fields(&fields, columns));
        Schema::new(fields)
    }
}

/// Immutable LLM evaluation scores attached to traces, spans, or sessions.
pub struct ScoreTable;

impl ScoreTable {
    pub fn table_name() -> &'static str {
        SCORES.name
    }

    pub fn schema() -> Schema {
        scores_base_schema().clone()
    }
}

/// Append-only score schemas for annotation and evaluators.
pub struct ScoreConfigTable;

impl ScoreConfigTable {
    pub fn table_name() -> &'static str {
        SCORE_CONFIGS.name
    }

    pub fn schema() -> Schema {
        score_configs_base_schema().clone()
    }
}

/// OTLP logs table
pub struct OtlpLogsTable;

impl OtlpLogsTable {
    pub fn table_name() -> &'static str {
        LOGS.name
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let mut fields = logs_base_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.extend(promoted_fields(&fields, columns));
        Schema::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn hot_attribute_columns_use_map() {
        let traces = TraceTable::schema();
        assert!(matches!(
            traces.field_with_name("attributes").unwrap().data_type(),
            DataType::Map(_, _)
        ));
        assert!(matches!(
            traces.field_with_name("events").unwrap().data_type(),
            DataType::Utf8
        ));

        let logs = OtlpLogsTable::schema();
        assert!(matches!(
            logs.field_with_name("attributes").unwrap().data_type(),
            DataType::Map(_, _)
        ));
        assert!(matches!(
            logs.field_with_name("resource_attributes")
                .unwrap()
                .data_type(),
            DataType::Map(_, _)
        ));

        // Scores metadata stays MAP (out of hot-column scope).
        let scores = ScoreTable::schema();
        assert!(matches!(
            scores.field_with_name("metadata").unwrap().data_type(),
            DataType::Map(_, _)
        ));
        assert!(scores.field_with_name("workspace_id").is_ok());
        assert!(ScoreConfigTable::schema()
            .field_with_name("workspace_id")
            .is_ok());
        assert!(logs.field_with_name("workspace_id").is_ok());
    }

    #[test]
    fn hot_map_registry_covers_schema_columns() {
        use crate::storage::schema::variant::hot_map_columns;

        for (table, schema) in [
            ("traces", TraceTable::schema()),
            ("logs", OtlpLogsTable::schema()),
        ] {
            for col in hot_map_columns(table) {
                let field = schema
                    .field_with_name(col)
                    .unwrap_or_else(|_| panic!("{table}.{col} missing from schema"));
                assert!(
                    matches!(field.data_type(), DataType::Map(_, _)),
                    "{table}.{col} must stage as MAP(VARCHAR, VARCHAR)"
                );
            }
        }
    }

    #[test]
    fn logs_timestamps_use_nanosecond_contract() {
        let schema = OtlpLogsTable::schema();

        assert_eq!(
            schema.field_with_name("timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            schema
                .field_with_name("observed_timestamp")
                .unwrap()
                .data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn all_ducklake_timestamps_use_nanoseconds() {
        let timestamp_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        for (table, schema) in [
            ("traces", TraceTable::schema()),
            ("logs", OtlpLogsTable::schema()),
            ("scores", ScoreTable::schema()),
            ("score_configs", ScoreConfigTable::schema()),
        ] {
            assert_eq!(
                schema.field_with_name("timestamp").unwrap().data_type(),
                &timestamp_ns,
                "{table}.timestamp"
            );
        }
        assert_eq!(
            TraceTable::schema()
                .field_with_name("end_timestamp")
                .unwrap()
                .data_type(),
            &timestamp_ns
        );
    }
}
