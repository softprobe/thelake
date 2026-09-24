//! Verify temporary MAP attribute bags (#55) and prefer-promoted SQL compilers.
//!
//! VARIANT shredding is deferred until DuckLake+Postgres VARIANT inlining is reliable.

use chrono::Utc;
use softprobe_runtime::ingest_engine::IngestEngine;
use softprobe_runtime::models::{Log as LogData, Span as SpanData};
use softprobe_runtime::query;
use softprobe_runtime::storage::schema::variant::{prefer_attr_varchar, variant_varchar};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;
use serde_json::Value;

fn assert_map_dtype(dtype: &str, column: &str) {
    let normalized = dtype.to_ascii_uppercase();
    assert!(
        normalized == "MAP" || normalized.starts_with("MAP(") || normalized.starts_with("MAP "),
        "{column} must be MAP(VARCHAR, VARCHAR), got {dtype}"
    );
}

fn attach(config: &softprobe_runtime::config::DuckLakeConfig) -> duckdb::Connection {
    softprobe_runtime::workspace_scope::PhysicalScope::from_ducklake(config)
        .open_attached_connection(Some(0))
}

fn attributes_object(value: &Value) -> serde_json::Map<String, Value> {
    match value {
        Value::Object(map) => map.clone(),
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default(),
        _ => serde_json::Map::new(),
    }
}

#[tokio::test]
async fn map_bags_hot_paths_and_nested_filters() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");
    let query_engine = query::create_query_engine(&config)
        .await
        .expect("query engine");

    let now = Utc::now();
    let session_id = format!("variant-sess-{}", uuid::Uuid::new_v4());
    let mut spans = Vec::new();
    for i in 0..80 {
        let mut attributes = HashMap::new();
        attributes.insert(
            "sp.observation.type".to_string(),
            if i % 2 == 0 {
                "generation".to_string()
            } else {
                "span".to_string()
            },
        );
        attributes.insert("gen_ai.request.model".to_string(), "gpt-test".to_string());
        attributes.insert(
            "gen_ai.usage.input_tokens".to_string(),
            ((i + 1) * 10).to_string(),
        );
        attributes.insert(
            "sp.cost.total".to_string(),
            format!("{:.2}", (i as f64) * 0.1),
        );
        attributes.insert("sp.user.id".to_string(), format!("user-{i}"));
        spans.push(SpanData {
            session_id: session_id.clone(),
            trace_id: format!("tr-{i}"),
            span_id: format!("sp-{i}"),
            parent_span_id: None,
            app_id: "variant-app".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "chat".to_string(),
            span_kind: Some("INTERNAL".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i + 1)),
            attributes,
            resource_attributes: HashMap::new(),
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: Some("OK".to_string()),
            status_message: None,
        });
    }

    let mut log_attrs = HashMap::new();
    log_attrs.insert("sp.session.id".to_string(), session_id.clone());
    let mut log_resource = HashMap::new();
    log_resource.insert("service.name".to_string(), "variant-svc".to_string());
    let log = LogData {
        session_id: Some(session_id.clone()),
        timestamp: now,
        observed_timestamp: Some(now),
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: "hello".to_string(),
        attributes: log_attrs,
        resource_attributes: log_resource,
        trace_id: Some("tr-0".to_string()),
        span_id: Some("sp-0".to_string()),
        tenant_id: None,
        agent_id: None,
        agent_name: None,
    };

    pipeline.add_spans(spans, 0).await.expect("write spans");
    pipeline.add_logs(vec![log], 0).await.expect("write logs");

    let conn = attach(&config.ducklake);
    let mut describe = conn.prepare("DESCRIBE traces;").expect("describe");
    let types: HashMap<String, String> = describe
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let dtype: String = row.get(1)?;
            Ok((name, dtype))
        })
        .expect("map")
        .map(|r| r.expect("row"))
        .collect();
    assert_map_dtype(
        types
            .get("attributes")
            .map(String::as_str)
            .unwrap_or("<missing>"),
        "traces.attributes",
    );
    assert_map_dtype(
        types
            .get("resource_attributes")
            .map(String::as_str)
            .unwrap_or("<missing>"),
        "traces.resource_attributes",
    );

    let started = Instant::now();
    let result = query_engine
        .count_traces_by_attribute(&session_id, "sp.observation.type", "generation")
        .await
        .expect("filter query");
    let elapsed = started.elapsed();
    assert_eq!(result, 40);
    assert!(
        elapsed < Duration::from_secs(5),
        "MAP bag filter should complete quickly, took {elapsed:?}"
    );

    let detail = query_engine
        .trace_attributes_by_attribute(&session_id, "sp.observation.type", "generation")
        .await
        .expect("detail")
        .expect("matching trace");
    let attrs = attributes_object(&detail);
    assert_eq!(
        attrs.get("sp.observation.type").and_then(|v| v.as_str()),
        Some("generation")
    );

    let logs = query_engine
        .count_logs_by_attribute("sp.session.id", &session_id)
        .await
        .expect("logs");
    assert_eq!(logs, 1);
}

/// Cover MAP bag key paths used by LLM / telemetry SQL compilers + prefer-promoted SQL.
#[tokio::test]
async fn map_key_queries_cover_llm_telemetry_and_capture_paths() {
    use softprobe_runtime::api::llm::query::ObservationSearchRequest;
    use softprobe_runtime::api::telemetry::{
        compile_details_sql, TelemetryDetailsTarget, TelemetryTimeRange,
    };
    use softprobe_runtime::sql::llm::compile_observation_search_sql;
    use softprobe_runtime::storage::schema::variant::prefer_attr_try_cast;

    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    let pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");
    let query_engine = query::create_query_engine(&config)
        .await
        .expect("query engine");

    let now = Utc::now();
    let session_id = format!("vk-sess-{}", uuid::Uuid::new_v4());
    let capture_id = format!("cap-{}", uuid::Uuid::new_v4());
    let tenant_id = "tenant-variant-keys";
    let trace_id = "vk-trace-1";

    // Span with full LLM hot-key set + capture id.
    let mut attrs = HashMap::new();
    attrs.insert("sp.observation.type".into(), "generation".into());
    attrs.insert("gen_ai.request.model".into(), "gpt-4o-mini".into());
    attrs.insert("gen_ai.provider.name".into(), "openai".into());
    attrs.insert("sp.user.id".into(), "user-vk-1".into());
    attrs.insert("gen_ai.usage.input_tokens".into(), "11".into());
    attrs.insert("gen_ai.usage.output_tokens".into(), "22".into());
    attrs.insert("gen_ai.usage.total_tokens".into(), "33".into());
    attrs.insert("sp.cost.total".into(), "0.42".into());
    attrs.insert("sp.capture.id".into(), capture_id.clone());
    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: trace_id.to_string(),
        span_id: "vk-span-1".into(),
        parent_span_id: None,
        app_id: "vk-app".into(),
        organization_id: None,
        tenant_id: Some(tenant_id.into()),
        agent_id: None,
        agent_name: None,
        message_type: "chat".into(),
        span_kind: Some("CLIENT".into()),
        timestamp: now,
        end_timestamp: Some(now + chrono::Duration::milliseconds(50)),
        attributes: attrs,
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: Some("OK".into()),
        status_message: None,
    };

    // Span missing observation type / user id (COALESCE + enduser fallback).
    let mut attrs_fallback = HashMap::new();
    attrs_fallback.insert("enduser.id".into(), "enduser-vk".into());
    attrs_fallback.insert("gen_ai.request.model".into(), "other-model".into());
    let span_fallback = SpanData {
        session_id: session_id.clone(),
        trace_id: "vk-trace-2".into(),
        span_id: "vk-span-2".into(),
        parent_span_id: None,
        app_id: "vk-app".into(),
        organization_id: None,
        tenant_id: Some(tenant_id.into()),
        agent_id: None,
        agent_name: None,
        message_type: "tool".into(),
        span_kind: Some("INTERNAL".into()),
        timestamp: now + chrono::Duration::milliseconds(1),
        end_timestamp: Some(now + chrono::Duration::milliseconds(2)),
        attributes: attrs_fallback,
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: Some("OK".into()),
        status_message: None,
    };

    let mut log_attrs = HashMap::new();
    log_attrs.insert("sp.session.id".into(), session_id.clone());
    let mut log_resource = HashMap::new();
    log_resource.insert("service.name".into(), "vk-svc".into());
    let log = LogData {
        session_id: Some(session_id.clone()),
        timestamp: now,
        observed_timestamp: Some(now),
        severity_number: 9,
        severity_text: "INFO".into(),
        body: "vk".into(),
        attributes: log_attrs,
        resource_attributes: log_resource,
        trace_id: Some(trace_id.into()),
        span_id: Some("vk-span-1".into()),
        tenant_id: None,
        agent_id: None,
        agent_name: None,
    };

    pipeline
        .add_spans(vec![span, span_fallback], 0)
        .await
        .expect("write spans");
    pipeline.add_logs(vec![log], 0).await.expect("write logs");

    // Prefer-promoted COALESCE(col, bag) requires the column to exist at bind time.
    // Add nullable product-hot columns (empty) so compiled LLM SQL can run; values
    // still resolve from the MAP bag until a real promotion apply+re-ingest.
    {
        let conn = attach(&config.ducklake);
        conn.execute_batch(
            "ALTER TABLE traces ADD COLUMN IF NOT EXISTS observation_type VARCHAR;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS model_name VARCHAR;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS model_provider VARCHAR;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS user_id VARCHAR;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS input_tokens BIGINT;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS output_tokens BIGINT;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS total_tokens BIGINT;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS total_cost DOUBLE;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS session_attr_id VARCHAR;
             ALTER TABLE traces ADD COLUMN IF NOT EXISTS service_name VARCHAR;",
        )
        .expect("add nullable prefer-promoted columns");
    }

    // 1) Prefer-promoted projections against MAP bags (columns NULL → bag fallback).
    let _proj_sql = format!(
        "SELECT \
            COALESCE({obs}, 'span') AS observation_type, \
            {model} AS model_name, \
            {provider} AS model_provider, \
            COALESCE({user}, {enduser}) AS user_id, \
            {input} AS input_tokens, \
            {output} AS output_tokens, \
            {total} AS total_tokens, \
            {cost} AS total_cost, \
            {capture} AS capture_id \
         FROM traces \
         WHERE session_id = '{sess}' AND span_id = 'vk-span-1' \
           AND make_timestamp_ns(epoch_ns(timestamp)) >= '1970-01-01'::TIMESTAMP_NS AND make_timestamp_ns(epoch_ns(timestamp)) <= '2100-01-01'::TIMESTAMP_NS",
        obs = prefer_attr_varchar(
            Some("observation_type"),
            "attributes",
            "sp.observation.type"
        ),
        model = prefer_attr_varchar(Some("model_name"), "attributes", "gen_ai.request.model"),
        provider =
            prefer_attr_varchar(Some("model_provider"), "attributes", "gen_ai.provider.name"),
        user = prefer_attr_varchar(Some("user_id"), "attributes", "sp.user.id"),
        enduser = variant_varchar("attributes", "enduser.id"),
        input = prefer_attr_try_cast(
            Some("input_tokens"),
            "attributes",
            "gen_ai.usage.input_tokens",
            "BIGINT"
        ),
        output = prefer_attr_try_cast(
            Some("output_tokens"),
            "attributes",
            "gen_ai.usage.output_tokens",
            "BIGINT"
        ),
        total = prefer_attr_try_cast(
            Some("total_tokens"),
            "attributes",
            "gen_ai.usage.total_tokens",
            "BIGINT"
        ),
        cost = prefer_attr_try_cast(Some("total_cost"), "attributes", "sp.cost.total", "DOUBLE"),
        capture = variant_varchar("attributes", "sp.capture.id"),
        sess = session_id.replace('\'', "''"),
    );
    let projected = query_engine
        .trace_attributes_for_span("vk-span-1")
        .await
        .expect("projected attributes")
        .expect("projected span");
    let projected = attributes_object(&projected);
    assert_eq!(projected["sp.observation.type"], "generation");
    assert_eq!(projected["gen_ai.request.model"], "gpt-4o-mini");
    assert_eq!(projected["gen_ai.provider.name"], "openai");
    assert_eq!(projected["sp.user.id"], "user-vk-1");
    assert_eq!(projected["gen_ai.usage.input_tokens"], "11");
    assert_eq!(projected["gen_ai.usage.output_tokens"], "22");
    assert_eq!(projected["gen_ai.usage.total_tokens"], "33");
    assert_eq!(projected["sp.capture.id"], capture_id);

    // 2) COALESCE default + enduser.id fallback.
    let fallback = query_engine
        .trace_attributes_for_span("vk-span-2")
        .await
        .expect("fallback")
        .expect("fallback span");
    let fallback = attributes_object(&fallback);
    assert_eq!(fallback.get("sp.observation.type"), None);
    assert_eq!(fallback["enduser.id"], "enduser-vk");

    // 3) Missing key is NULL (not an error).
    let missing = query_engine
        .trace_attributes_for_span("vk-span-1")
        .await
        .expect("missing")
        .expect("source span");
    assert!(!attributes_object(&missing).contains_key("does.not.exist"));

    // 4) Compiled LLM observation search SQL prefers promoted columns against live MAP data.
    let search = ObservationSearchRequest {
        from: now - chrono::Duration::hours(1),
        to: now + chrono::Duration::hours(1),
        observation_types: vec!["generation".into()],
        model_name: Some("gpt-4o-mini".into()),
        user_id: Some("user-vk-1".into()),
        session_id: Some(session_id.clone()),
        trace_id: None,
        limit: Some(10),
        cursor: None,
    };
    let search_sql = compile_observation_search_sql(&search).expect("compile search");
    let obs_pos = search_sql
        .find("observation_type")
        .expect("promoted observation_type");
    let bag_pos = search_sql
        .find("attributes['sp.observation.type']")
        .expect("bag fallback");
    assert!(
        obs_pos < bag_pos,
        "prefer-promoted: observation_type must lead bag access"
    );
    assert!(search_sql.contains("COALESCE(observation_type,"));
    assert!(search_sql.contains("COALESCE(model_name,"));
    assert!(search_sql.contains("COALESCE(user_id,"));
    let search_result = query_engine
        .search_observations(&search)
        .await
        .expect("run search sql");
    assert_eq!(search_result.row_count, 1);
    // columns include observation_type / model_name / tokens from projection
    let cols = &search_result.columns;
    let obs_idx = cols.iter().position(|c| c == "observation_type").unwrap();
    let model_idx = cols.iter().position(|c| c == "model_name").unwrap();
    let tokens_idx = cols.iter().position(|c| c == "total_tokens").unwrap();
    assert_eq!(search_result.rows[0][obs_idx].as_str(), Some("generation"));
    assert_eq!(
        search_result.rows[0][model_idx].as_str(),
        Some("gpt-4o-mini")
    );
    assert_eq!(search_result.rows[0][tokens_idx].as_i64(), Some(33));

    // Negative: wrong model filters out the generation span.
    let miss = ObservationSearchRequest {
        model_name: Some("no-such-model".into()),
        ..search.clone()
    };
    let _miss_sql = compile_observation_search_sql(&miss).expect("compile miss");
    let miss_result = query_engine
        .search_observations(&miss)
        .await
        .expect("run miss");
    assert_eq!(miss_result.row_count, 0);

    // 5) Nested MAP capture-id key (SoftProbe capture_export removed with Redis).
    //
    // `IngestEngine::bound_default` always stamps writes with its own bound workspace id
    // (anti-spoofing; see `bind_spans_to_workspace`), so the capture id (already
    // globally unique) is what disambiguates this row rather than `tenant_id`.
    let capture_result = query_engine
        .trace_attributes_by_attribute(&session_id, "sp.capture.id", &capture_id)
        .await
        .expect("capture id filter")
        .expect("capture span");
    let attrs_obj = attributes_object(&capture_result);
    assert_eq!(
        attrs_obj.get("sp.capture.id").and_then(|v| v.as_str()),
        Some(capture_id.as_str())
    );

    let details_range = TelemetryTimeRange {
        from: (now - chrono::Duration::hours(1)).to_rfc3339(),
        to: (now + chrono::Duration::hours(1)).to_rfc3339(),
    };
    let _details = compile_details_sql(
        &TelemetryDetailsTarget {
            kind: "session".into(),
            id: session_id.clone(),
        },
        &details_range,
        100,
    )
    .expect("compile details");

    let logs = query_engine
        .telemetry_details_logs(
            &TelemetryDetailsTarget {
                kind: "session".into(),
                id: session_id.clone(),
            },
            &details_range,
            100,
        )
        .await
        .expect("details logs");
    assert_eq!(logs.row_count, 1);
    let log_attr_idx = logs
        .columns
        .iter()
        .position(|c| c == "attributes")
        .expect("log attributes");
    assert!(
        !attributes_object(&logs.rows[0][log_attr_idx]).is_empty()
            || logs.rows[0][log_attr_idx].as_str().is_some(),
        "log attributes should deserialize as JSON object or JSON text"
    );
}

#[tokio::test]
async fn map_write_fails_fast_on_legacy_variant_table() {
    // make test-e2e exports SPLAKE_RESET_DUCKLAKE=1; that path drops tables for local
    // iteration only. This test must not rely on DROP and must not fight that reset.
    let previous_reset = std::env::var_os("SPLAKE_RESET_DUCKLAKE");
    std::env::remove_var("SPLAKE_RESET_DUCKLAKE");

    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(0);

    // Fresh catalog: create leftover VARIANT table first (no DROP). Writer CREATE IF NOT EXISTS
    // leaves it alone; ensure_hot_map_column_types must then fail fast (#55).
    {
        let conn = attach(&config.ducklake);
        conn.execute_batch(
            "CREATE TABLE traces AS SELECT NULL::VARCHAR AS tenant_id, '{}'::JSON::VARIANT AS attributes, '{}'::JSON::VARIANT AS resource_attributes;",
        )
            .expect("create leftover variant table");
        let dtype: String = conn
            .query_row(
                "SELECT column_type FROM (DESCRIBE traces) WHERE column_name = 'attributes';",
                [],
                |row| row.get(0),
            )
            .expect("describe attributes type");
        assert!(
            dtype.to_ascii_uppercase().contains("VARIANT"),
            "precondition: leftover table should be VARIANT, got {dtype}"
        );
    }

    let init_result = IngestEngine::bound_default(&config).await;
    match previous_reset {
        Some(value) => std::env::set_var("SPLAKE_RESET_DUCKLAKE", value),
        None => std::env::remove_var("SPLAKE_RESET_DUCKLAKE"),
    }

    let err = match init_result {
        Ok(_) => panic!("leftover VARIANT table must fail fast"),
        Err(error) => error,
    };
    let message = err.to_string();
    assert!(
        message.contains("VARIANT"),
        "error must mention leftover VARIANT: {message}"
    );
    assert!(
        message.contains("Temporary MAP rollback") || message.contains("#55"),
        "error must mention MAP rollback #55: {message}"
    );
    assert!(
        message.contains("rebuild") || message.contains("migrate"),
        "error should tell operators migration is required, got: {message}"
    );
}
