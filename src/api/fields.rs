use crate::api::error::{bad_request, storage_error, ApiError};
use crate::api::mapping::resolve_engine;
use crate::api::{map_execute_result, AppState};
use crate::authn::TenantInfo;
use crate::sql::telemetry::{
    field_spec, field_values as field_values_sql, query_window_from_time_range, TelemetryTimeRange,
    SEARCH_FIELDS,
};
use crate::sql::QueryWindow;
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use serde_json::{json, Value};
use std::collections::HashMap;

pub async fn fields() -> Json<Value> {
    Json(json!({
        "version": 1,
        "fields": SEARCH_FIELDS.iter().map(|field| json!({
            "key": field.key,
            "type": field.value_type,
            "entity": field.entity,
            "filterable": field.filterable,
            "sortable": field.sortable,
            "projectable": field.projectable,
            "operators": field.ops,
        })).collect::<Vec<_>>()
    }))
}

pub async fn field_values(
    State(state): State<AppState>,
    tenant: Option<Extension<TenantInfo>>,
    Path(field): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let spec = field_spec(&field).ok_or_else(|| bad_request("unknown field"))?;
    if !spec.filterable {
        return Err(bad_request("field is not filterable"));
    }
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(500)
        .clamp(1, 10_000);
    let window = parse_field_values_window(&params).map_err(bad_request)?;
    let trusted = field_values_sql(spec.sql, &window, limit).map_err(bad_request)?;
    let engine = resolve_engine(&state, tenant.as_ref().map(|e| &e.0)).await?;
    let result =
        map_execute_result(engine.execute_trusted(trusted).await).map_err(storage_error)?;
    let values = result
        .rows
        .iter()
        .filter_map(|row| row.first().cloned())
        .collect::<Vec<_>>();
    Ok(Json(
        json!({ "version": 1, "field": field, "values": values }),
    ))
}

fn parse_field_values_window(params: &HashMap<String, String>) -> Result<QueryWindow, String> {
    let from = params
        .get("from")
        .ok_or_else(|| "`from` is required".to_string())?;
    let to = params
        .get("to")
        .ok_or_else(|| "`to` is required".to_string())?;
    query_window_from_time_range(&TelemetryTimeRange {
        from: from.clone(),
        to: to.clone(),
    })
    .map_err(|e| {
        e.replace("timeRange.from", "from")
            .replace("timeRange.to", "to")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_values_without_window_rejected() {
        let empty = HashMap::new();
        let err = parse_field_values_window(&empty).unwrap_err();
        assert!(err.contains("`from` is required"));

        let mut only_from = HashMap::new();
        only_from.insert("from".into(), "2023-11-14T22:13:20Z".into());
        let err = parse_field_values_window(&only_from).unwrap_err();
        assert!(err.contains("`to` is required"));
    }
}
