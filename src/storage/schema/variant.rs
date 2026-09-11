//! Temporary MAP-era helpers for hot telemetry attribute bags (#55).
//!
//! Hot bags are stored as DuckLake `MAP(VARCHAR, VARCHAR)`. VARIANT shredding is
//! deferred until external-catalog VARIANT inlining is reliable again.

use serde_json::{Map as JsonMap, Number, Value};
use std::collections::HashMap;

/// Attribute keys encoded as JSON integers for stable typed encoding (metrics labels).
pub const VARIANT_INT64_KEYS: &[&str] = &[
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.total_tokens",
];

/// Attribute keys encoded as JSON floats for stable typed encoding (metrics labels).
pub const VARIANT_FLOAT64_KEYS: &[&str] = &["sp.cost.total"];

/// Telemetry columns stored as DuckLake `MAP(VARCHAR, VARCHAR)`.
pub fn hot_map_columns(table_name: &str) -> &'static [&'static str] {
    match table_name {
        "traces" => &[
            "attributes",
            "resource_attributes",
            "instrumentation_scope",
            "links",
        ],
        "logs" => &["attributes", "resource_attributes"],
        // Skinny metric_samples have no attribute bags; series labels are the hot MAP.
        "metric_series" => &["labels"],
        _ => &[],
    }
}

/// Deprecated alias for [`hot_map_columns`].
#[deprecated(note = "renamed to hot_map_columns (#55 temporary MAP era)")]
pub fn hot_variant_columns(table_name: &str) -> &'static [&'static str] {
    hot_map_columns(table_name)
}

/// Escape a SQL string literal (single quotes only).
pub fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// MAP/VARIANT object field as VARCHAR (required for COALESCE / string filters).
pub fn variant_varchar(column: &str, key: &str) -> String {
    format!(
        "CAST({column}['{key}'] AS VARCHAR)",
        column = column,
        key = escape_sql_string(key)
    )
}

/// `try_cast` of a MAP/VARIANT object field to a DuckDB type.
pub fn variant_try_cast(column: &str, key: &str, duck_type: &str) -> String {
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
    let from_bag = variant_varchar(bag, key);
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
    let from_bag = variant_try_cast(bag, key, duck_type);
    match promoted {
        Some(col) => format!("COALESCE({col}, {from_bag})"),
        None => from_bag,
    }
}

/// Project a MAP/VARIANT column as JSON for API serialization.
pub fn variant_as_json(column: &str) -> String {
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

/// Rehydrate nested JSON-text values inside a projected MAP object.
///
/// `MAP(VARCHAR, VARCHAR)` cannot store typed nested values; after
/// `CAST(map AS JSON)` nested OTel payloads appear as JSON strings. Walk the
/// object and parse stringified arrays/objects (and `sp.json:` prefixed forms).
pub fn rehydrate_map_json_values(value: Value) -> Value {
    let top = parse_projected_json_value(value);
    match top {
        Value::Object(map) => {
            let mut out = JsonMap::new();
            for (k, v) in map {
                out.insert(k, rehydrate_map_json_values(v));
            }
            Value::Object(out)
        }
        Value::String(text) => {
            let payload = crate::models::strip_nested_json_prefix(&text).unwrap_or(text.as_str());
            match serde_json::from_str::<Value>(payload) {
                Ok(parsed @ (Value::Object(_) | Value::Array(_))) => parsed,
                Ok(other) if crate::models::strip_nested_json_prefix(&text).is_some() => other,
                _ => Value::String(text),
            }
        }
        other => other,
    }
}

/// Flatten a JSON object (or JSON-text object) into string map entries for label projection.
///
/// Empty keys and JSON null values are dropped. Non-object input yields an empty map.
pub fn variant_json_to_string_map(value: &Value) -> HashMap<String, String> {
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

/// DuckLake SELECT list for Parquet ingest (MAP columns need no cast bridge).
pub fn parquet_select_for_table(_table_name: &str) -> String {
    "SELECT *".to_string()
}

/// Deprecated alias for [`parquet_select_for_table`].
#[deprecated(note = "renamed to parquet_select_for_table (#55); always SELECT *")]
pub fn parquet_select_with_variant_casts(table_name: &str) -> String {
    parquet_select_for_table(table_name)
}

/// Encode a string map as a JSON object, applying stable typed shredding for hot keys.
pub fn encode_attributes_json(map: &HashMap<String, String>) -> String {
    let mut obj = JsonMap::new();
    for (key, value) in map {
        obj.insert(key.clone(), typed_json_value(key, value));
    }
    Value::Object(obj).to_string()
}

fn typed_json_value(key: &str, value: &str) -> Value {
    if VARIANT_INT64_KEYS.contains(&key) {
        if let Ok(n) = value.parse::<i64>() {
            return Value::Number(Number::from(n));
        }
    }
    if VARIANT_FLOAT64_KEYS.contains(&key) {
        if let Ok(n) = value.parse::<f64>() {
            if let Some(num) = Number::from_f64(n) {
                return Value::Number(num);
            }
        }
    }
    // Only rehydrate values explicitly tagged by any_value encoding (arrays/kvlists).
    // Plain OTLP StringValues that look like JSON stay strings.
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
    fn encodes_hot_keys_with_stable_json_types() {
        let mut map = HashMap::new();
        map.insert("sp.observation.type".into(), "generation".into());
        map.insert("gen_ai.usage.input_tokens".into(), "42".into());
        map.insert("sp.cost.total".into(), "1.5".into());
        map.insert("sp.user.id".into(), "u1".into());

        let json: Value = serde_json::from_str(&encode_attributes_json(&map)).unwrap();
        assert_eq!(json["sp.observation.type"], "generation");
        assert_eq!(json["gen_ai.usage.input_tokens"], 42);
        assert_eq!(json["sp.cost.total"], 1.5);
        assert_eq!(json["sp.user.id"], "u1");
    }

    #[test]
    fn variant_sql_helpers_cast_nested_fields() {
        assert_eq!(
            variant_varchar("attributes", "sp.user.id"),
            "CAST(attributes['sp.user.id'] AS VARCHAR)"
        );
        assert_eq!(
            variant_try_cast("attributes", "gen_ai.usage.input_tokens", "BIGINT"),
            "try_cast(attributes['gen_ai.usage.input_tokens'] AS BIGINT)"
        );
        assert_eq!(parquet_select_for_table("traces"), "SELECT *");
        assert_eq!(parquet_select_for_table("logs"), "SELECT *");
        assert_eq!(parquet_select_for_table("scores"), "SELECT *");
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
