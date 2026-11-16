//! Helpers for telemetry attributes stored in DuckLake VARIANT columns.

use serde_json::{Map as JsonMap, Value};
use std::collections::HashMap;

/// Telemetry columns stored as DuckLake `VARIANT`.
pub fn hot_map_columns(table_name: &str) -> &'static [&'static str] {
    match table_name {
        "traces" => &[
            "attributes",
            "resource_attributes",
            "instrumentation_scope",
            "links",
        ],
        "logs" => &["attributes", "resource_attributes"],
        _ => &[],
    }
}

/// Whether `table_name.column_name` is stored as DuckLake `VARIANT`.
pub fn is_variant_column(table_name: &str, column_name: &str) -> bool {
    match table_name {
        "traces" => matches!(
            column_name,
            "attributes" | "resource_attributes" | "instrumentation_scope" | "links" | "events"
        ),
        "logs" => matches!(column_name, "attributes" | "resource_attributes"),
        "scores" | "score_configs" => column_name == "metadata",
        _ => false,
    }
}

/// Escape a SQL string literal (single quotes only).
pub fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// VARIANT object field as VARCHAR (required for COALESCE / string filters).
pub fn attribute_map_varchar(column: &str, key: &str) -> String {
    format!(
        "CAST({column}['{key}'] AS VARCHAR)",
        column = column,
        key = escape_sql_string(key)
    )
}

/// `try_cast` of a VARIANT object field to a DuckDB type.
pub fn attribute_map_try_cast(column: &str, key: &str, duck_type: &str) -> String {
    format!(
        "try_cast({column}['{key}'] AS {duck_type})",
        column = column,
        key = escape_sql_string(key),
        duck_type = duck_type
    )
}

/// Prefer a promoted column when present; otherwise read from the attribute bag.
///
/// `promoted` must already be a safe SQL identifier (caller-validated).
pub fn prefer_attr_varchar(promoted: Option<&str>, bag: &str, key: &str) -> String {
    let from_bag = attribute_map_varchar(bag, key);
    match promoted {
        Some(col) => format!("COALESCE({col}, {from_bag})"),
        None => from_bag,
    }
}

/// Prefer a promoted column when present; otherwise `try_cast` from the attribute bag.
///
/// `promoted` must already be a safe SQL identifier (caller-validated).
pub fn prefer_attr_try_cast(
    promoted: Option<&str>,
    bag: &str,
    key: &str,
    duck_type: &str,
) -> String {
    let from_bag = attribute_map_try_cast(bag, key, duck_type);
    match promoted {
        Some(col) => format!("COALESCE({col}, {from_bag})"),
        None => from_bag,
    }
}

/// Project a VARIANT column as JSON for API serialization.
pub fn attribute_map_as_json(column: &str) -> String {
    format!("CAST({column} AS JSON) AS {column}")
}

/// DuckDB returns `CAST(... AS JSON)` as text; parse to object/array when possible.
///
/// Leaves non-JSON strings and non-string values unchanged so callers can treat
/// projections as nested JSON without double-encoding in HTTP responses.
pub fn parse_projected_json_value(value: Value) -> Value {
    match value {
        Value::String(text) => match serde_json::from_str::<Value>(&text) {
            Ok(parsed @ (Value::Object(_) | Value::Array(_))) => parsed,
            _ => Value::String(text),
        },
        other => other,
    }
}

/// Flatten a JSON object (or JSON-text object) into string map entries for label projection.
///
/// Empty keys and JSON null values are dropped. Non-object input yields an empty map.
pub fn attribute_map_json_to_string_map(value: &Value) -> HashMap<String, String> {
    let parsed = parse_projected_json_value(value.clone());
    let map = match parsed {
        Value::Object(map) => map,
        _ => return HashMap::new(),
    };
    map.iter()
        .filter_map(|(k, v)| {
            if k.is_empty() {
                return None;
            }
            let s = match v {
                Value::Null => return None,
                Value::String(t) => t.clone(),
                other => other.to_string(),
            };
            Some((k.clone(), s))
        })
        .collect()
}

/// DuckLake projection for Parquet ingest into `VARIANT` columns.
pub fn parquet_select_for_table(table_name: &str) -> String {
    match table_name {
        "traces" => "SELECT * REPLACE (\
            TRY_CAST(attributes AS JSON)::VARIANT AS attributes, \
            TRY_CAST(resource_attributes AS JSON)::VARIANT AS resource_attributes, \
            TRY_CAST(instrumentation_scope AS JSON)::VARIANT AS instrumentation_scope, \
            TRY_CAST(links AS JSON)::VARIANT AS links, \
            TRY_CAST(events AS JSON)::VARIANT AS events)"
            .to_string(),
        "logs" => "SELECT * REPLACE (\
            TRY_CAST(attributes AS JSON)::VARIANT AS attributes, \
            TRY_CAST(resource_attributes AS JSON)::VARIANT AS resource_attributes)"
            .to_string(),
        "scores" | "score_configs" => {
            "SELECT * REPLACE (TRY_CAST(metadata AS JSON)::VARIANT AS metadata)".to_string()
        }
        _ => "SELECT *".to_string(),
    }
}

/// Encode a string map as JSON for `VARIANT` staging.
pub fn encode_attributes_json(map: &HashMap<String, String>) -> String {
    let mut obj = JsonMap::new();
    for (key, value) in map {
        obj.insert(key.clone(), typed_json_value(value));
    }
    Value::Object(obj).to_string()
}

fn typed_json_value(value: &str) -> Value {
    if let Some(payload) = crate::models::strip_nested_json_prefix(value) {
        match serde_json::from_str::<Value>(payload) {
            Ok(parsed @ (Value::Object(_) | Value::Array(_))) => return parsed,
            Ok(other) => return other,
            Err(_) => return Value::String(value.to_string()),
        }
    }
    Value::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_map_sql_helpers_cast_nested_fields() {
        assert_eq!(
            attribute_map_varchar("attributes", "sp.user.id"),
            "CAST(attributes['sp.user.id'] AS VARCHAR)"
        );
        assert_eq!(
            attribute_map_try_cast("attributes", "gen_ai.usage.input_tokens", "BIGINT"),
            "try_cast(attributes['gen_ai.usage.input_tokens'] AS BIGINT)"
        );
        assert!(
            parquet_select_for_table("traces").contains("TRY_CAST(attributes AS JSON)::VARIANT")
        );
        assert!(parquet_select_for_table("traces").contains("TRY_CAST(events AS JSON)::VARIANT"));
        assert!(parquet_select_for_table("logs").contains("TRY_CAST(attributes AS JSON)::VARIANT"));
        assert_eq!(
            parquet_select_for_table("scores"),
            "SELECT * REPLACE (TRY_CAST(metadata AS JSON)::VARIANT AS metadata)"
        );
    }

    #[test]
    fn prefer_attr_helpers_promoted_first() {
        assert_eq!(
            prefer_attr_varchar(Some("attr_user_id"), "attributes", "sp.user.id"),
            "COALESCE(attr_user_id, CAST(attributes['sp.user.id'] AS VARCHAR))"
        );
        assert_eq!(
            prefer_attr_varchar(None, "attributes", "sp.user.id"),
            "CAST(attributes['sp.user.id'] AS VARCHAR)"
        );
        assert_eq!(
            prefer_attr_try_cast(
                Some("attr_tokens"),
                "attributes",
                "gen_ai.usage.input_tokens",
                "BIGINT"
            ),
            "COALESCE(attr_tokens, try_cast(attributes['gen_ai.usage.input_tokens'] AS BIGINT))"
        );
        assert_eq!(
            prefer_attr_try_cast(None, "attributes", "gen_ai.usage.input_tokens", "BIGINT"),
            "try_cast(attributes['gen_ai.usage.input_tokens'] AS BIGINT)"
        );

        let promoted = prefer_attr_varchar(Some("p"), "attributes", "k");
        assert!(promoted.starts_with("COALESCE(p,"));
        assert!(!promoted.starts_with("CAST(attributes"));
    }

    #[test]
    fn parse_projected_json_value_objects_and_leaves_plain_text() {
        let obj = parse_projected_json_value(Value::String(
            r#"{"logger_name":"agent.transform"}"#.to_string(),
        ));
        assert_eq!(obj["logger_name"], "agent.transform");

        let plain = parse_projected_json_value(Value::String("not-json".to_string()));
        assert_eq!(plain, Value::String("not-json".to_string()));
    }

    #[test]
    fn encode_attributes_json_rehydrates_only_tagged_nested() {
        let mut map = HashMap::new();
        map.insert("tags".into(), r#"sp.json:["a",1]"#.into());
        map.insert("meta".into(), r#"sp.json:{"k":false}"#.into());
        map.insert("plain".into(), "hello".into());
        map.insert("looks_like_json".into(), r#"{"a":1}"#.into());
        let json: Value = serde_json::from_str(&encode_attributes_json(&map)).unwrap();
        assert_eq!(json["tags"], serde_json::json!(["a", 1]));
        assert_eq!(json["meta"], serde_json::json!({"k": false}));
        assert_eq!(json["plain"], "hello");
        assert_eq!(json["looks_like_json"], r#"{"a":1}"#);
    }
}
