use crate::promotion::{PromotionColumn, PromotionDataType};
use crate::storage::schema::variant::hot_map_columns;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::sync::Arc;

fn utf8() -> DataType {
    DataType::Utf8
}

fn ts_utc() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
}

fn ts_utc_nanos() -> DataType {
    // DuckDB's timezone-bearing TIMESTAMP is microsecond precision. Loki and Tempo
    // expose Unix nanoseconds, so those tables use timezone-free TIMESTAMP_NS.
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn string_map() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", utf8(), false),
                Field::new("value", utf8(), true),
            ])),
            false,
        )),
        false,
    )
}

fn double_map() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", utf8(), false),
                Field::new("value", DataType::Float64, true),
            ])),
            false,
        )),
        false,
    )
}

/// Nullable hot MAP field; must be registered in [`hot_map_columns`].
fn opt_hot_map(table: &str, name: &'static str) -> Field {
    assert!(
        hot_map_columns(table).contains(&name),
        "column '{name}' must be listed in hot_map_columns(\"{table}\")"
    );
    opt(name, string_map())
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
                PromotionDataType::Timestamp => ts_utc(),
            };
            Field::new(&column.name, data_type, true)
        })
        .collect()
}

fn req(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, false)
}

fn opt(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, true)
}

/// Raw sessions table - stores OTLP spans
pub struct TraceTable;

impl TraceTable {
    pub fn table_name() -> &'static str {
        "traces"
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let events_element = DataType::Struct(Fields::from(vec![
            req("name", utf8()),
            req("timestamp", ts_utc_nanos()),
            opt("attributes", string_map()),
        ]));
        let mut fields = vec![
            req("session_id", utf8()),
            req("trace_id", utf8()),
            req("span_id", utf8()),
            opt("parent_span_id", utf8()),
            req("app_id", utf8()),
            opt("organization_id", utf8()),
            opt("tenant_id", utf8()),
            req("message_type", utf8()),
            opt("span_kind", utf8()),
            req("timestamp", ts_utc_nanos()),
            opt("end_timestamp", ts_utc_nanos()),
            opt_hot_map("traces", "attributes"),
            opt_hot_map("traces", "resource_attributes"),
            opt_hot_map("traces", "instrumentation_scope"),
            opt_hot_map("traces", "links"),
            opt(
                "events",
                DataType::List(Arc::new(Field::new("item", events_element, true))),
            ),
            opt("status_code", utf8()),
            opt("status_message", utf8()),
            opt("http_request_method", utf8()),
            opt("http_request_path", utf8()),
            opt("http_request_headers", utf8()),
            opt("http_request_body", utf8()),
            opt("http_response_status_code", DataType::Int32),
            opt("http_response_headers", utf8()),
            opt("http_response_body", utf8()),
            req("record_date", DataType::Date32),
            // Product-hot nullable columns (#55). Append after core fields so
            // Arrow builders that fill by legacy position stay aligned.
            // Present before promotion apply so prefer-promoted COALESCE is safe.
            opt("observation_type", utf8()),
            opt("model_name", utf8()),
            opt("model_provider", utf8()),
            opt("user_id", utf8()),
            opt("input_tokens", DataType::Int64),
            opt("output_tokens", DataType::Int64),
            opt("total_tokens", DataType::Int64),
            opt("total_cost", DataType::Float64),
            opt("session_attr_id", utf8()),
            opt("service_name", utf8()),
            // Softprobe assertion agent identity (auth-stamped; not client OTLP).
            opt("agent_id", utf8()),
            opt("agent_name", utf8()),
        ];
        fields.extend(promoted_fields(&fields, columns));
        Schema::new(fields)
    }
}

/// Per-ingest skinny session stats for merge-on-read list (`sessions/search`).
pub struct SessionStatsDeltaTable;

impl SessionStatsDeltaTable {
    pub fn table_name() -> &'static str {
        "session_stats_delta"
    }

    pub fn schema() -> Schema {
        Schema::new(vec![
            req("session_id", utf8()),
            req("record_date", DataType::Date32),
            req("start_time", ts_utc_nanos()),
            req("end_time", ts_utc_nanos()),
            req("observation_count", DataType::Int64),
            req("error_count", DataType::Int64),
            req("trace_count", DataType::Int64),
            req("input_tokens", DataType::Int64),
            req("output_tokens", DataType::Int64),
            req("total_tokens", DataType::Int64),
            req("total_cost", DataType::Float64),
            opt("agent_name", utf8()),
            req("is_nested_child", DataType::Boolean),
            opt("measures", double_map()),
        ])
    }
}

/// Immutable LLM evaluation scores attached to traces, spans, or sessions.
pub struct ScoreTable;

impl ScoreTable {
    pub fn table_name() -> &'static str {
        "scores"
    }

    pub fn schema() -> Schema {
        Schema::new(vec![
            req("score_id", utf8()),
            req("timestamp", ts_utc()),
            opt("trace_id", utf8()),
            opt("span_id", utf8()),
            opt("session_id", utf8()),
            req("name", utf8()),
            req("data_type", utf8()),
            opt("numeric_value", DataType::Float64),
            opt("string_value", utf8()),
            opt("boolean_value", DataType::Boolean),
            req("source", utf8()),
            opt("comment", utf8()),
            opt("config_id", utf8()),
            opt("author_id", utf8()),
            opt("metadata", string_map()),
            req("record_date", DataType::Date32),
        ])
    }
}

/// Append-only score schemas for annotation and evaluators.
pub struct ScoreConfigTable;

impl ScoreConfigTable {
    pub fn table_name() -> &'static str {
        "score_configs"
    }

    pub fn schema() -> Schema {
        Schema::new(vec![
            req("config_id", utf8()),
            req("timestamp", ts_utc()),
            req("name", utf8()),
            req("data_type", utf8()),
            opt("description", utf8()),
            opt("min_value", DataType::Float64),
            opt("max_value", DataType::Float64),
            // JSON array string for categorical allowed values (simple DuckLake round-trip).
            opt("categories", utf8()),
            opt("author_id", utf8()),
            opt("metadata", string_map()),
            req("record_date", DataType::Date32),
        ])
    }
}

/// OTLP logs table
pub struct OtlpLogsTable;

impl OtlpLogsTable {
    pub fn table_name() -> &'static str {
        "logs"
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let mut fields = vec![
            opt("session_id", utf8()),
            // Loki's public log contract is nanoseconds since Unix epoch.
            req("timestamp", ts_utc_nanos()),
            opt("observed_timestamp", ts_utc_nanos()),
            req("severity_number", DataType::Int32),
            req("severity_text", utf8()),
            req("body", utf8()),
            opt_hot_map("logs", "attributes"),
            opt_hot_map("logs", "resource_attributes"),
            opt("trace_id", utf8()),
            opt("span_id", utf8()),
            req("record_date", DataType::Date32),
            // Product-hot nullable columns (#55). Append after core fields.
            opt("logger_name", utf8()),
            opt("service_name", utf8()),
            opt("deployment_environment", utf8()),
            opt("session_attr_id", utf8()),
            opt("user_id", utf8()),
            // Softprobe assertion agent identity (auth-stamped; not client OTLP).
            opt("agent_id", utf8()),
            opt("agent_name", utf8()),
        ];
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
        // Nested event attributes remain MAP.
        let events = traces.field_with_name("events").unwrap().data_type();
        let DataType::List(item) = events else {
            panic!("expected list");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("expected struct");
        };
        let attrs = fields.iter().find(|f| f.name() == "attributes").unwrap();
        assert!(matches!(attrs.data_type(), DataType::Map(_, _)));

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
    fn session_stats_delta_has_core_columns_and_measures_map() {
        let schema = SessionStatsDeltaTable::schema();
        assert_eq!(SessionStatsDeltaTable::table_name(), "session_stats_delta");
        for name in [
            "session_id",
            "record_date",
            "start_time",
            "end_time",
            "observation_count",
            "error_count",
            "trace_count",
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "total_cost",
            "agent_name",
            "is_nested_child",
            "measures",
        ] {
            assert!(
                schema.field_with_name(name).is_ok(),
                "missing column {name}"
            );
        }
        assert!(matches!(
            schema.field_with_name("measures").unwrap().data_type(),
            DataType::Map(_, _)
        ));
        let DataType::Map(entries, _) = schema.field_with_name("measures").unwrap().data_type()
        else {
            panic!("expected map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected struct entries");
        };
        assert_eq!(fields.find("value").unwrap().1.data_type(), &DataType::Float64);
        assert_eq!(
            schema.field_with_name("start_time").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }
}
