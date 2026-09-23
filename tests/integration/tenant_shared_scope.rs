//! Shared physical-scope contract: authenticated workspace writes and typed
//! queries stay isolated when two workspaces use one DuckLake scope.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use chrono::Utc;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use softprobe_runtime::async_jobs::{LeaseStore, PostgresLeaseStore};
use softprobe_runtime::authn::TenantInfo;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::{Log, Score, ScoreConfig, ScoreDataType, ScoreSource, Span};
use softprobe_runtime::runtime_api::runtime_control_routes;
use softprobe_runtime::runtime_engine::ScopeProvisioningRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;

fn shared_config(temp: &TempDir, registry_schema: String) -> Config {
    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.shrink_pools_for_tests();
    config.query.max_connections = 2;
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into());
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.metadata_schema = registry_schema;
    config.ducklake.data_path = temp.path().join("shared_data").to_string_lossy().into();
    config.ducklake.workspace_scope_mode =
        softprobe_runtime::workspace_scope::WorkspaceScopeMode::Shared;
    config.ducklake.data_inlining_row_limit = Some(0);
    config.ingest.flush_interval_seconds = 0;
    config
}

fn tenant(id: &str) -> TenantInfo {
    TenantInfo {
        tenant_id: id.to_string(),
        bucket_name: "shared-scope-test".to_string(),
        dataset_id: "shared-scope-test".to_string(),
        agent_id: None,
        agent_name: None,
    }
}

fn span(tenant_id: &str, trace_id: &str, session_id: &str) -> Span {
    Span {
        session_id: session_id.to_string(),
        trace_id: trace_id.to_string(),
        span_id: format!("span-{trace_id}"),
        parent_span_id: None,
        app_id: "shared-scope-test".to_string(),
        organization_id: None,
        tenant_id: Some(tenant_id.to_string()),
        agent_id: None,
        agent_name: Some(format!("agent-{tenant_id}")),
        message_type: "op".to_string(),
        span_kind: Some("SPAN_KIND_INTERNAL".to_string()),
        timestamp: Utc::now(),
        end_timestamp: None,
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        status_code: None,
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    }
}

fn log(tenant_id: &str, trace_id: &str, session_id: &str) -> Log {
    Log {
        session_id: Some(session_id.to_string()),
        timestamp: Utc::now(),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: format!("log-{tenant_id}"),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(trace_id.to_string()),
        span_id: Some(format!("span-{trace_id}")),
        tenant_id: Some(tenant_id.to_string()),
        agent_id: None,
        agent_name: None,
    }
}

fn score_config(config_id: &str, workspace_id: &str) -> ScoreConfig {
    ScoreConfig {
        config_id: config_id.to_string(),
        timestamp: Utc::now(),
        name: "quality".to_string(),
        data_type: ScoreDataType::Numeric,
        description: Some(format!("shared scope test {workspace_id}")),
        min_value: Some(0.0),
        max_value: Some(1.0),
        categories: Vec::new(),
        author_id: None,
        metadata: HashMap::new(),
        tenant_id: None,
    }
}

fn score(score_id: &str, trace_id: &str, config_id: &str, workspace_id: &str) -> Score {
    Score {
        score_id: score_id.to_string(),
        timestamp: Utc::now(),
        trace_id: Some(trace_id.to_string()),
        span_id: None,
        session_id: None,
        name: "quality".to_string(),
        data_type: ScoreDataType::Numeric,
        numeric_value: Some(0.9),
        string_value: None,
        boolean_value: None,
        source: ScoreSource::Evaluator,
        comment: Some(format!("score for {workspace_id}")),
        config_id: Some(config_id.to_string()),
        author_id: None,
        metadata: HashMap::new(),
        tenant_id: None,
    }
}

async fn json_response(response: axum::response::Response<Body>) -> (StatusCode, Value) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("read response body")
        .to_bytes();
    let value = serde_json::from_slice(&body).unwrap_or_else(|error| {
        panic!(
            "JSON response status={status}: {error}; body={}",
            String::from_utf8_lossy(&body)
        )
    });
    (status, value)
}

async fn typed_details(router: &Router, workspace: &str, trace_id: &str) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/telemetry/details")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "version": 1,
                "target": {"kind": "trace", "id": trace_id},
                "timeRange": {
                    "from": "2020-01-01T00:00:00Z",
                    "to": "2030-01-01T00:00:00Z"
                },
                "limit": 100
            })
            .to_string(),
        ))
        .expect("request");
    request.extensions_mut().insert(tenant(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn rebuild_summary(router: &Router, workspace: &str) -> (StatusCode, Value) {
    let from = Utc::now() - chrono::Duration::days(1);
    let to = Utc::now() + chrono::Duration::days(1);
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/llm/sessions/summary/rebuild")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "from": from,
                "to": to
            })
            .to_string(),
        ))
        .expect("summary rebuild request");
    request.extensions_mut().insert(tenant(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn search_sessions(router: &Router, workspace: &str) -> (StatusCode, Value) {
    let from = Utc::now() - chrono::Duration::days(1);
    let to = Utc::now() + chrono::Duration::days(1);
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/llm/sessions/search")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "from": from,
                "to": to,
                "limit": 100
            })
            .to_string(),
        ))
        .expect("session search request");
    request.extensions_mut().insert(tenant(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn get_compat_json(router: &Router, workspace: &str, uri: String) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("compatibility request");
    request.extensions_mut().insert(tenant(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn list_score_configs(router: &Router, workspace: &str) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri("/v1/llm/score-configs")
        .body(Body::empty())
        .expect("score config list request");
    request.extensions_mut().insert(tenant(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn get_trace(router: &Router, workspace: &str, trace_id: &str) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/llm/traces/{trace_id}?from=2020-01-01T00:00:00Z&to=2030-01-01T00:00:00Z"
        ))
        .body(Body::empty())
        .expect("trace request");
    request.extensions_mut().insert(tenant(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

#[tokio::test]
async fn shared_scope_stamps_writes_and_filters_typed_queries_per_workspace() {
    let dsn = "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake";
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .expect("shared scope contract requires ducklake-postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    drop(client);

    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let config = shared_config(&temp, format!("shared_scope_contract_{suffix}"));
    let (router, state) = softprobe_runtime::api::create_router(
        Arc::new(config.clone()),
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("shared router");
    let engines = &state.engines;
    let shared_schema = config.ducklake.metadata_schema.clone();
    let shared_data = config.ducklake.data_path.clone();
    let workspace_a = format!("shared_a_{suffix}");
    let workspace_b = format!("shared_b_{suffix}");
    for workspace in [&workspace_a, &workspace_b] {
        engines
            .provision_scope(ScopeProvisioningRequest {
                scope_id: workspace.to_string(),
                metadata_schema: shared_schema.clone(),
                data_path: shared_data.clone(),
            })
            .await
            .expect("provision shared workspace");
    }

    let (engine_a, engine_b) = tokio::try_join!(
        state.engines.engine_for(&workspace_a),
        state.engines.engine_for(&workspace_b),
    )
    .expect("concurrent engines");
    let trace_a = format!("a{}", &suffix[..31]);
    let trace_b = format!("b{}", &suffix[..31]);
    let shared_session = format!("shared-session-{suffix}");

    tokio::try_join!(
        engine_a.add_spans(vec![span(&workspace_a, &trace_a, &shared_session)], 0),
        engine_b.add_spans(vec![span(&workspace_b, &trace_b, &shared_session)], 0),
        engine_a.add_logs(vec![log(&workspace_a, &trace_a, &shared_session)], 0),
        engine_b.add_logs(vec![log(&workspace_b, &trace_b, &shared_session)], 0),
    )
    .expect("concurrent shared writes");

    let config_id = format!("shared-config-{suffix}");
    tokio::try_join!(
        engine_a.add_score_configs(vec![score_config(&config_id, &workspace_a)]),
        engine_b.add_score_configs(vec![score_config(&config_id, &workspace_b)]),
    )
    .expect("concurrent shared score-config writes");
    tokio::try_join!(
        engine_a.add_scores(vec![score(
            "shared-score",
            &trace_a,
            &config_id,
            &workspace_a,
        )]),
        engine_b.add_scores(vec![score(
            "shared-score",
            &trace_b,
            &config_id,
            &workspace_b,
        )]),
    )
    .expect("concurrent shared score writes");

    for _ in 0..4 {
        let (status_a, details_a) = typed_details(&router, &workspace_a, &trace_a).await;
        assert_eq!(status_a, StatusCode::OK, "workspace A query: {details_a}");
        assert_eq!(details_a["spans"].as_array().unwrap().len(), 1);
        assert_eq!(details_a["logs"].as_array().unwrap().len(), 1);
        assert_eq!(details_a["spans"][0]["trace_id"], trace_a);
        assert_eq!(details_a["logs"][0]["body"], format!("log-{workspace_a}"));

        let (status_b, details_b) = typed_details(&router, &workspace_b, &trace_a).await;
        assert_eq!(status_b, StatusCode::OK, "workspace B query: {details_b}");
        assert!(details_b["spans"].as_array().unwrap().is_empty());
        assert!(details_b["logs"].as_array().unwrap().is_empty());
    }

    assert!(engine_a.score_exists("shared-score").await.unwrap());
    assert!(engine_b.score_exists("shared-score").await.unwrap());
    assert!(engine_a.score_config_exists(&config_id).await.unwrap());
    assert!(engine_b.score_config_exists(&config_id).await.unwrap());

    let (config_status_a, configs_a) = list_score_configs(&router, &workspace_a).await;
    assert_eq!(
        config_status_a,
        StatusCode::OK,
        "score configs A: {configs_a}"
    );
    assert_eq!(configs_a["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        configs_a["items"][0]["description"],
        format!("shared scope test {workspace_a}")
    );
    let (config_status_b, configs_b) = list_score_configs(&router, &workspace_b).await;
    assert_eq!(
        config_status_b,
        StatusCode::OK,
        "score configs B: {configs_b}"
    );
    assert_eq!(configs_b["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        configs_b["items"][0]["description"],
        format!("shared scope test {workspace_b}")
    );

    let (trace_status_a, trace_result_a) = get_trace(&router, &workspace_a, &trace_a).await;
    assert_eq!(trace_status_a, StatusCode::OK, "trace A: {trace_result_a}");
    assert_eq!(trace_result_a["scores"].as_array().unwrap().len(), 1);
    assert_eq!(
        trace_result_a["scores"][0]["comment"],
        format!("score for {workspace_a}")
    );
    let (trace_status_b, trace_result_b) = get_trace(&router, &workspace_b, &trace_a).await;
    assert_eq!(
        trace_status_b,
        StatusCode::NOT_FOUND,
        "trace B: {trace_result_b}"
    );
    let (own_trace_status_b, own_trace_result_b) = get_trace(&router, &workspace_b, &trace_b).await;
    assert_eq!(
        own_trace_status_b,
        StatusCode::OK,
        "own trace B: {own_trace_result_b}"
    );
    assert_eq!(own_trace_result_b["scores"].as_array().unwrap().len(), 1);
    assert_eq!(
        own_trace_result_b["scores"][0]["comment"],
        format!("score for {workspace_b}")
    );

    let (rebuild_status_a, rebuild_a) = rebuild_summary(&router, &workspace_a).await;
    assert_eq!(rebuild_status_a, StatusCode::OK, "summary A: {rebuild_a}");
    let (rebuild_status_b, rebuild_b) = rebuild_summary(&router, &workspace_b).await;
    assert_eq!(rebuild_status_b, StatusCode::OK, "summary B: {rebuild_b}");
    let (search_status_a, sessions_a) = search_sessions(&router, &workspace_a).await;
    assert_eq!(search_status_a, StatusCode::OK, "session A: {sessions_a}");
    assert_eq!(sessions_a["items"].as_array().unwrap().len(), 1);
    assert_eq!(sessions_a["items"][0]["session_id"], shared_session);
    assert_eq!(
        sessions_a["items"][0]["agent_name"],
        format!("agent-{workspace_a}")
    );
    let (search_status_b, sessions_b) = search_sessions(&router, &workspace_b).await;
    assert_eq!(search_status_b, StatusCode::OK, "session B: {sessions_b}");
    assert_eq!(sessions_b["items"].as_array().unwrap().len(), 1);
    assert_eq!(sessions_b["items"][0]["session_id"], shared_session);
    assert_eq!(
        sessions_b["items"][0]["agent_name"],
        format!("agent-{workspace_b}")
    );

    let now_seconds = Utc::now().timestamp();
    let (tempo_status_a, tempo_a) = get_compat_json(
        &router,
        &workspace_a,
        format!(
            "/api/search?start={}&end={}&limit=20",
            now_seconds - 86_400,
            now_seconds + 86_400
        ),
    )
    .await;
    assert_eq!(tempo_status_a, StatusCode::OK, "Tempo A: {tempo_a}");
    assert!(tempo_a["traces"]
        .as_array()
        .unwrap()
        .iter()
        .any(|trace| trace["traceID"] == trace_a));
    let (tempo_status_b, tempo_b) = get_compat_json(
        &router,
        &workspace_b,
        format!(
            "/api/search?start={}&end={}&limit=20",
            now_seconds - 86_400,
            now_seconds + 86_400
        ),
    )
    .await;
    assert_eq!(tempo_status_b, StatusCode::OK, "Tempo B: {tempo_b}");
    assert!(!tempo_b["traces"]
        .as_array()
        .unwrap()
        .iter()
        .any(|trace| trace["traceID"] == trace_a));

    let start_ns = Utc::now().timestamp_nanos_opt().unwrap() - 86_400_000_000_000;
    let end_ns = start_ns + 172_800_000_000_000;
    let (loki_status_a, loki_a) = get_compat_json(
        &router,
        &workspace_a,
        format!("/loki/api/v1/query_range?query=%7B%7D&start={start_ns}&end={end_ns}&limit=100"),
    )
    .await;
    assert_eq!(loki_status_a, StatusCode::OK, "Loki A: {loki_a}");
    assert_eq!(loki_a["data"]["result"].as_array().unwrap().len(), 1);
    assert!(loki_a.to_string().contains(&format!("log-{workspace_a}")));
    let (loki_status_b, loki_b) = get_compat_json(
        &router,
        &workspace_b,
        format!("/loki/api/v1/query_range?query=%7B%7D&start={start_ns}&end={end_ns}&limit=100"),
    )
    .await;
    assert_eq!(loki_status_b, StatusCode::OK, "Loki B: {loki_b}");
    assert_eq!(loki_b["data"]["result"].as_array().unwrap().len(), 1);
    assert!(loki_b.to_string().contains(&format!("log-{workspace_b}")));

    let leases = PostgresLeaseStore::from_engines(&state.engines);
    let (lease_a, lease_b) = tokio::join!(
        leases.try_acquire(
            "workspace_session_summary_rebuild",
            &workspace_a,
            "shared-contract-holder-a",
            Duration::from_secs(60),
        ),
        leases.try_acquire(
            "workspace_session_summary_rebuild",
            &workspace_a,
            "shared-contract-holder-b",
            Duration::from_secs(60),
        )
    );
    let lease_a = lease_a.expect("workspace lease A");
    let lease_b = lease_b.expect("workspace lease B");
    assert_ne!(lease_a, lease_b, "one workspace lease must have one winner");
    let winner = if lease_a {
        "shared-contract-holder-a"
    } else {
        "shared-contract-holder-b"
    };
    leases
        .release("workspace_session_summary_rebuild", &workspace_a, winner)
        .await
        .expect("release workspace lease");

    let maintenance = state
        .engines
        .maintenance_engine()
        .await
        .expect("shared maintenance engine");
    let maintenance_summary = maintenance
        .run_pass(false)
        .await
        .expect("shared physical maintenance pass");
    let trace_maintenance_entries = maintenance_summary
        .tables
        .iter()
        .filter(|table| table.table.ends_with(".traces"))
        .count();
    assert_eq!(
        trace_maintenance_entries, 1,
        "one physical maintenance result for shared traces: {maintenance_summary:?}"
    );

    let mut raw_request = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "sql": format!(
                    "SELECT * FROM {}.{}.traces",
                    config.ducklake.catalog_alias,
                    config.ducklake.metadata_schema
                )
            })
            .to_string(),
        ))
        .expect("raw SQL request");
    raw_request.extensions_mut().insert(tenant(&workspace_a));
    let (raw_status, raw_body) =
        json_response(router.clone().oneshot(raw_request).await.unwrap()).await;
    assert_eq!(raw_status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(raw_body
        .to_string()
        .contains("shared_scope_raw_sql_forbidden"));

    let control_router = runtime_control_routes().with_state(state.clone());
    let promotion_manifest = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [logs]
columns:
  - name: shared_scope_test_column
    type: string
    nullable: true
    source:
      from: attribute
      key: shared.scope.test
"#;
    let mut promotion_request = Request::builder()
        .method("POST")
        .uri("/v1/promotions/apply")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"manifestYaml": promotion_manifest}).to_string(),
        ))
        .expect("promotion request");
    promotion_request
        .extensions_mut()
        .insert(tenant(&workspace_a));
    let (promotion_status, promotion_body) = json_response(
        control_router
            .clone()
            .oneshot(promotion_request)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(promotion_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        promotion_body["error"]["code"],
        "shared_scope_promotion_unsupported"
    );

    let mut connection_request = Request::builder()
        .method("GET")
        .uri("/v1/data/ducklake-connection")
        .body(Body::empty())
        .expect("connection request");
    connection_request
        .extensions_mut()
        .insert(tenant(&workspace_a));
    let (connection_status, connection_body) =
        json_response(control_router.oneshot(connection_request).await.unwrap()).await;
    assert_eq!(connection_status, StatusCode::CONFLICT);
    assert_eq!(
        connection_body["error"]["code"],
        "shared_scope_connection_unavailable"
    );
}
