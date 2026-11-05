//! Shared physical-scope contract: authenticated workspace writes and typed
//! queries stay isolated when two workspaces use one DuckLake scope.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use chrono::Utc;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use softprobe_runtime::async_jobs::{LeaseStore, PostgresLeaseStore};
use softprobe_runtime::authn::WorkspaceAuth;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::{Log, Score, ScoreConfig, ScoreDataType, ScoreSource, Span};
use softprobe_runtime::promotion::{parse_promotion_manifest, PromotionManifest};
use softprobe_runtime::query::TraceCountFilter;
use softprobe_runtime::workspace::{ScopeProvisioningRequest, WorkspaceContext, WorkspaceManager};
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

fn mock_auth(id: &str) -> WorkspaceAuth {
    WorkspaceAuth {
        workspace_id: id.to_string(),
        bucket_name: "shared-scope-test".to_string(),
        dataset_id: "shared-scope-test".to_string(),
        agent_id: None,
        agent_name: None,
    }
}

fn span(workspace_id: &str, trace_id: &str, session_id: &str) -> Span {
    Span {
        session_id: session_id.to_string(),
        trace_id: trace_id.to_string(),
        span_id: format!("span-{trace_id}"),
        parent_span_id: None,
        app_id: "shared-scope-test".to_string(),
        organization_id: None,
        workspace_id: Some(workspace_id.to_string()),
        agent_id: None,
        agent_name: Some(format!("agent-{workspace_id}")),
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

fn log(workspace_id: &str, trace_id: &str, session_id: &str) -> Log {
    Log {
        session_id: Some(session_id.to_string()),
        timestamp: Utc::now(),
        observed_timestamp: None,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: format!("log-{workspace_id}"),
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        trace_id: Some(trace_id.to_string()),
        span_id: Some(format!("span-{trace_id}")),
        workspace_id: Some(workspace_id.to_string()),
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
        workspace_id: Some(workspace_id.to_string()),
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
        workspace_id: Some(workspace_id.to_string()),
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
        .uri("/v1/traces/details")
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
    request.extensions_mut().insert(mock_auth(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn rebuild_summary(router: &Router, workspace: &str) -> (StatusCode, Value) {
    let from = Utc::now() - chrono::Duration::days(1);
    let to = Utc::now() + chrono::Duration::days(1);
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/sessions/summary/rebuild")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "from": from,
                "to": to
            })
            .to_string(),
        ))
        .expect("summary rebuild request");
    request.extensions_mut().insert(mock_auth(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn search_sessions(router: &Router, workspace: &str) -> (StatusCode, Value) {
    let from = Utc::now() - chrono::Duration::days(1);
    let to = Utc::now() + chrono::Duration::days(1);
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/sessions/search")
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
    request.extensions_mut().insert(mock_auth(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn get_compat_json(router: &Router, workspace: &str, uri: String) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("compatibility request");
    request.extensions_mut().insert(mock_auth(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn list_score_configs(router: &Router, workspace: &str) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri("/v1/score-configs")
        .body(Body::empty())
        .expect("score config list request");
    request.extensions_mut().insert(mock_auth(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn get_trace(router: &Router, workspace: &str, trace_id: &str) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/traces/{trace_id}?from=2020-01-01T00:00:00Z&to=2030-01-01T00:00:00Z"
        ))
        .body(Body::empty())
        .expect("trace request");
    request.extensions_mut().insert(mock_auth(workspace));
    json_response(router.clone().oneshot(request).await.expect("route")).await
}

async fn ducklake_relation_exists(schema: &str, relation: &str) -> bool {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("connect DuckLake catalog");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sql = format!(
        r#"SELECT count(*) FROM "{}".ducklake_table WHERE table_name = $1;"#,
        schema.replace('"', "\"\"")
    );
    client
        .query_one(&sql, &[&relation])
        .await
        .expect("query DuckLake relation metadata")
        .get::<_, i64>(0)
        == 1
}

#[tokio::test]
async fn shared_scope_stamps_writes_filters_queries_and_shares_promotions() {
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
    let (router, state) = softprobe_runtime::api::create_router(Arc::new(config.clone()), None)
        .await
        .expect("shared router");
    let workspaces = &state.workspaces;
    let peer_workspaces = WorkspaceManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("second workspace manager for same registry");
    let shared_schema = config.ducklake.metadata_schema.clone();
    let shared_data = config.ducklake.data_path.clone();
    let workspace_a = Uuid::new_v4().to_string();
    let workspace_b = Uuid::new_v4().to_string();
    tokio::try_join!(
        workspaces.provision_scope(ScopeProvisioningRequest {
            scope_id: workspace_a.to_string(),
            metadata_schema: shared_schema.clone(),
            data_path: shared_data.clone(),
        }),
        workspaces.provision_scope(ScopeProvisioningRequest {
            scope_id: workspace_b.to_string(),
            metadata_schema: shared_schema.clone(),
            data_path: shared_data.clone(),
        }),
    )
    .expect("concurrent provision of workspaces in one physical scope");

    let (ws_a, peer_ws_b) = tokio::try_join!(
        state.workspaces.workspace_for(&workspace_a),
        peer_workspaces.workspace_for(&workspace_b),
    )
    .expect("concurrent workspaces");
    // Keep score deduplication assertions within one writer pool; cross-manager
    // initialization and concurrent telemetry writes are exercised above.
    let ws_b = workspaces
        .workspace_for(&workspace_b)
        .await
        .expect("workspace B context");
    let trace_a = format!("a{}", &suffix[..31]);
    let trace_b = format!("b{}", &suffix[..31]);
    let shared_session = format!("shared-session-{suffix}");

    tokio::try_join!(
        ws_a.ingest()
            .add_spans(vec![span(&workspace_a, &trace_a, &shared_session)], 0),
        peer_ws_b
            .ingest()
            .add_spans(vec![span(&workspace_b, &trace_b, &shared_session)], 0),
        ws_a.ingest()
            .add_logs(vec![log(&workspace_a, &trace_a, &shared_session)], 0),
        peer_ws_b
            .ingest()
            .add_logs(vec![log(&workspace_b, &trace_b, &shared_session)], 0),
    )
    .expect("concurrent shared writes");

    let config_id = format!("shared-config-{suffix}");
    ws_a.ingest()
        .add_score_configs(vec![score_config(&config_id, &workspace_a)])
        .await
        .expect("shared score-config write A");
    ws_b.ingest()
        .add_score_configs(vec![score_config(&config_id, &workspace_b)])
        .await
        .expect("shared score-config write B");
    let score_a = score("shared-score", &trace_a, &config_id, &workspace_a);
    let score_a_timestamp = score_a.timestamp;
    ws_a.ingest()
        .add_scores(vec![score_a])
        .await
        .expect("shared score write A");
    let score_b = score("shared-score", &trace_b, &config_id, &workspace_b);
    let score_b_timestamp = score_b.timestamp;
    ws_b.ingest()
        .add_scores(vec![score_b])
        .await
        .expect("shared score write B");

    let visibility_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (status_a, details_a) = typed_details(&router, &workspace_a, &trace_a).await;
        assert_eq!(status_a, StatusCode::OK, "workspace A query: {details_a}");
        let a_visible = details_a["spans"].as_array().unwrap().len() == 1
            && details_a["logs"].as_array().unwrap().len() == 1;

        let (status_b, details_b) = typed_details(&router, &workspace_b, &trace_a).await;
        assert_eq!(status_b, StatusCode::OK, "workspace B query: {details_b}");
        assert!(details_b["spans"].as_array().unwrap().is_empty());
        assert!(details_b["logs"].as_array().unwrap().is_empty());
        if a_visible {
            assert_eq!(details_a["spans"][0]["trace_id"], trace_a);
            assert_eq!(details_a["logs"][0]["body"], format!("log-{workspace_a}"));
            break;
        }
        assert!(
            tokio::time::Instant::now() < visibility_deadline,
            "workspace A writes never became query-visible: {details_a}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let score_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (score_a, score_b, config_a, config_b) = tokio::join!(
            ws_a.query().score_exists("shared-score", score_a_timestamp),
            ws_b.query().score_exists("shared-score", score_b_timestamp),
            ws_a.query().score_config_exists(&config_id),
            ws_b.query().score_config_exists(&config_id),
        );
        let score_visibility = (
            score_a.unwrap(),
            score_b.unwrap(),
            config_a.unwrap(),
            config_b.unwrap(),
        );
        if score_visibility.0 && score_visibility.1 && score_visibility.2 && score_visibility.3 {
            break;
        }
        assert!(tokio::time::Instant::now() < score_deadline,
            "shared score/config writes never became visible to both managers: {score_visibility:?}; configs A={:?}, B={:?}",
            ws_a.query().list_score_configs().await.unwrap(), ws_b.query().list_score_configs().await.unwrap());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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

    let leases = PostgresLeaseStore::from_workspaces(&state.workspaces);
    let (lease_a, lease_b) = tokio::join!(
        leases.acquire_lease(
            "workspace_session_summary_rebuild",
            &workspace_a,
            "shared-contract-holder-a",
            Duration::from_secs(60),
        ),
        leases.acquire_lease(
            "workspace_session_summary_rebuild",
            &workspace_a,
            "shared-contract-holder-b",
            Duration::from_secs(60),
        )
    );
    let lease_a = lease_a.expect("workspace lease A");
    let lease_b = lease_b.expect("workspace lease B");
    assert_ne!(
        lease_a.is_some(),
        lease_b.is_some(),
        "one workspace lease must have one winner"
    );
    let winner = lease_a.or(lease_b).expect("winner fencing token");
    leases
        .release_lease("workspace_session_summary_rebuild", &workspace_a, &winner)
        .await
        .expect("release workspace lease");

    let maintenance = state
        .workspaces
        .maintenance_engine()
        .await
        .expect("shared maintenance engine");
    maintenance
        .run_pass()
        .await
        .expect("shared physical maintenance pass");
    assert_eq!(
        maintenance
            .maintenance_scope_keys()
            .await
            .expect("scope keys")
            .len(),
        1,
        "shared tenants must schedule one SQL pass per physical warehouse"
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
    raw_request.extensions_mut().insert(mock_auth(&workspace_a));
    let (raw_status, raw_body) =
        json_response(router.clone().oneshot(raw_request).await.unwrap()).await;
    assert_eq!(raw_status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(raw_body
        .to_string()
        .contains("shared_scope_raw_sql_forbidden"));

    let control_router = router.clone();
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
        .insert(mock_auth(&workspace_a));
    let (promotion_status, promotion_body) = json_response(
        control_router
            .clone()
            .oneshot(promotion_request)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(promotion_status, StatusCode::OK, "{promotion_body}");
    assert_eq!(promotion_body["applied"], true);

    let promoted_trace_a = format!("promo-a-{suffix}");
    let promoted_trace_b = format!("promo-b-{suffix}");
    let promoted_value = "global-promotion-value";
    let mut promoted_log_a = log(&workspace_a, &promoted_trace_a, &shared_session);
    promoted_log_a
        .attributes
        .insert("shared.scope.test".to_string(), promoted_value.to_string());
    let mut promoted_log_b = log(&workspace_b, &promoted_trace_b, &shared_session);
    promoted_log_b
        .attributes
        .insert("shared.scope.test".to_string(), promoted_value.to_string());
    tokio::try_join!(
        ws_a.ingest().add_spans(
            vec![span(&workspace_a, &promoted_trace_a, &shared_session,)],
            0
        ),
        ws_b.ingest().add_spans(
            vec![span(&workspace_b, &promoted_trace_b, &shared_session,)],
            0
        ),
        ws_a.ingest().add_logs(vec![promoted_log_a], 0),
        ws_b.ingest().add_logs(vec![promoted_log_b], 0),
    )
    .expect("ingest through both shared workspaces after global promotion");

    let (promoted_status_a, promoted_details_a) =
        typed_details(&router, &workspace_a, &promoted_trace_a).await;
    assert_eq!(
        promoted_status_a,
        StatusCode::OK,
        "workspace A promoted query: {promoted_details_a}"
    );
    assert_eq!(
        promoted_details_a["logs"][0]["attributes"]["shared_scope_test_column"],
        promoted_value
    );
    let (promoted_status_b, promoted_details_b) =
        typed_details(&router, &workspace_b, &promoted_trace_b).await;
    assert_eq!(
        promoted_status_b,
        StatusCode::OK,
        "workspace B promoted query: {promoted_details_b}"
    );
    assert_eq!(
        promoted_details_b["logs"][0]["attributes"]["shared_scope_test_column"],
        promoted_value
    );

    let business_manifest = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: shared_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
"#;
    let PromotionManifest::BusinessTable(business_spec) =
        parse_promotion_manifest(business_manifest).expect("business promotion manifest")
    else {
        panic!("expected business promotion manifest");
    };
    let mut business_request = Request::builder()
        .method("POST")
        .uri("/v1/promotions/apply")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"manifestYaml": business_manifest}).to_string(),
        ))
        .expect("business promotion request");
    business_request
        .extensions_mut()
        .insert(mock_auth(&workspace_a));
    let (business_status, business_body) = json_response(
        control_router
            .clone()
            .oneshot(business_request)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(business_status, StatusCode::OK, "{business_body}");
    assert_eq!(business_body["applied"], true);
    assert!(
        ducklake_relation_exists(&shared_schema, "shared_orders_v1").await,
        "business table promoted by A must exist in B's physical scope"
    );
    assert!(
        ws_b.admin()
            .apply_business_promotion_guarded(business_manifest, &business_spec)
            .await
            .is_ok(),
        "workspace B can observe the shared business promotion"
    );

    let previous_reset = std::env::var_os("SPLAKE_RESET_DUCKLAKE");
    std::env::set_var("SPLAKE_RESET_DUCKLAKE", "1");
    let restarted_manager = WorkspaceManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("restart manager");
    let restarted_b = restarted_manager
        .workspace_for(&workspace_b)
        .await
        .expect("restart workspace B context");
    let restarted_trace = format!("restart-promo-{suffix}");
    let mut restarted_log = log(&workspace_b, &restarted_trace, &shared_session);
    restarted_log
        .attributes
        .insert("shared.scope.test".to_string(), promoted_value.to_string());
    restarted_b
        .ingest()
        .add_spans(
            vec![span(&workspace_b, &restarted_trace, &shared_session)],
            0,
        )
        .await
        .expect("restart span write");
    restarted_b
        .ingest()
        .add_logs(vec![restarted_log], 0)
        .await
        .expect("restart log write");
    let (restart_status, restart_details) =
        typed_details(&router, &workspace_b, &restarted_trace).await;
    assert_eq!(
        restart_status,
        StatusCode::OK,
        "restart query: {restart_details}"
    );
    assert_eq!(
        restart_details["logs"][0]["attributes"]["shared_scope_test_column"], promoted_value,
        "telemetry promotion must be replayed after reset"
    );
    assert!(
        ducklake_relation_exists(&shared_schema, "shared_orders_v1").await,
        "business promotion must be replayed after reset"
    );
    if let Some(value) = previous_reset {
        std::env::set_var("SPLAKE_RESET_DUCKLAKE", value);
    } else {
        std::env::remove_var("SPLAKE_RESET_DUCKLAKE");
    }

    let mut connection_request = Request::builder()
        .method("GET")
        .uri("/v1/data/ducklake-connection")
        .body(Body::empty())
        .expect("connection request");
    connection_request
        .extensions_mut()
        .insert(mock_auth(&workspace_a));
    let (connection_status, connection_body) =
        json_response(control_router.oneshot(connection_request).await.unwrap()).await;
    assert_eq!(connection_status, StatusCode::CONFLICT);
    assert_eq!(
        connection_body["error"]["code"],
        "shared_scope_connection_unavailable"
    );
}

struct ResetEnvGuard(Option<std::ffi::OsString>);

impl ResetEnvGuard {
    fn set() -> Self {
        let previous = std::env::var_os("SPLAKE_RESET_DUCKLAKE");
        std::env::set_var("SPLAKE_RESET_DUCKLAKE", "1");
        Self(previous)
    }
}

impl Drop for ResetEnvGuard {
    fn drop(&mut self) {
        match self.0.take() {
            Some(value) => std::env::set_var("SPLAKE_RESET_DUCKLAKE", value),
            None => std::env::remove_var("SPLAKE_RESET_DUCKLAKE"),
        }
    }
}

async fn provision_shared_pair(manager: &WorkspaceManager, config: &Config) -> (String, String) {
    let ids = provision_shared(manager, config, 2).await;
    (ids[0].clone(), ids[1].clone())
}

async fn provision_shared(
    manager: &WorkspaceManager,
    config: &Config,
    count: usize,
) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let workspace = Uuid::new_v4().to_string();
        manager
            .provision_scope(ScopeProvisioningRequest {
                scope_id: workspace.clone(),
                metadata_schema: config.ducklake.metadata_schema.clone(),
                data_path: config.ducklake.data_path.clone(),
            })
            .await
            .expect("provision shared workspace");
        ids.push(workspace);
    }
    ids
}

fn trace_id_for(index: usize, suffix: &str) -> String {
    format!("{index:x}{}", &suffix[..31])
}

async fn write_private_spans(
    contexts: &[Arc<WorkspaceContext>],
    ids: &[String],
    suffix: &str,
    session: &str,
) -> Vec<String> {
    let mut traces = Vec::with_capacity(ids.len());
    for (index, (workspace_id, workspace)) in ids.iter().zip(contexts).enumerate() {
        let trace_id = trace_id_for(index, suffix);
        workspace
            .ingest()
            .add_spans(vec![span(workspace_id, &trace_id, session)], 0)
            .await
            .expect("write private span");
        traces.push(trace_id);
    }
    for (workspace, trace_id) in contexts.iter().zip(&traces) {
        wait_until_visible(workspace, trace_id).await;
    }
    traces
}

async fn assert_spans_stay_private(contexts: &[Arc<WorkspaceContext>], traces: &[String]) {
    for (reader_index, reader) in contexts.iter().enumerate() {
        for (writer_index, trace_id) in traces.iter().enumerate() {
            let expected = u64::from(reader_index == writer_index);
            assert_eq!(
                visible_span_count(reader, trace_id).await,
                expected,
                "workspace {reader_index} reading span {writer_index}"
            );
        }
    }
}

async fn visible_span_count(workspace: &WorkspaceContext, trace_id: &str) -> u64 {
    workspace
        .query()
        .count_traces(TraceCountFilter {
            time_window: crate::util::query_window(),
            session_id: None,
            app_id: None,
            span_id: Some(format!("span-{trace_id}")),
        })
        .await
        .expect("typed span count")
}

async fn wait_until_visible(workspace: &WorkspaceContext, trace_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while visible_span_count(workspace, trace_id).await != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "span {trace_id} never became visible to its own workspace"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn shared_workspaces_share_one_catalog_pool_and_rebind_one_query_slot() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let mut config = shared_config(&temp, format!("shared_pool_contract_{suffix}"));
    config.ducklake.writer_pool_size = 1;
    config.query.max_connections = 1;
    let manager = WorkspaceManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("workspace manager");
    let (workspace_a, workspace_b) = provision_shared_pair(&manager, &config).await;

    let ws_a = manager.workspace_for(&workspace_a).await.expect("A");
    let ws_b = manager.workspace_for(&workspace_b).await.expect("B");

    assert_eq!(
        manager.catalog_pool_count_for_test(),
        1,
        "two shared workspaces must share one CatalogPool"
    );
    assert_eq!(
        manager.catalog_session_count_for_test(),
        1 + 1 + 1,
        "sessions = writer slots + query slots + one maintenance session"
    );

    let trace_a = format!("a{}", &suffix[..31]);
    let trace_b = format!("b{}", &suffix[..31]);
    let session = format!("pool-session-{suffix}");
    ws_a.ingest()
        .add_spans(vec![span(&workspace_a, &trace_a, &session)], 0)
        .await
        .expect("write A");
    ws_b.ingest()
        .add_spans(vec![span(&workspace_b, &trace_b, &session)], 0)
        .await
        .expect("write B");
    wait_until_visible(&ws_a, &trace_a).await;
    wait_until_visible(&ws_b, &trace_b).await;

    for round in 0..3 {
        assert_eq!(
            visible_span_count(&ws_a, &trace_a).await,
            1,
            "round {round}"
        );
        assert_eq!(
            visible_span_count(&ws_a, &trace_b).await,
            0,
            "A must not read B's span on the shared slot (round {round})"
        );
        assert_eq!(
            visible_span_count(&ws_b, &trace_b).await,
            1,
            "round {round}"
        );
        assert_eq!(
            visible_span_count(&ws_b, &trace_a).await,
            0,
            "B must not read A's span on the shared slot (round {round})"
        );
    }
    assert_eq!(
        manager.catalog_session_count_for_test(),
        1 + 1 + 1,
        "rebinding the query slot must not open sessions"
    );
}

#[tokio::test]
async fn dev_reset_wipes_once_when_the_shared_pool_is_created() {
    let _reset = ResetEnvGuard::set();
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let mut config = shared_config(&temp, format!("shared_reset_contract_{suffix}"));
    config.ducklake.writer_pool_size = 1;
    config.query.max_connections = 1;
    let manager = WorkspaceManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("workspace manager");
    let (workspace_a, workspace_b) = provision_shared_pair(&manager, &config).await;

    let ws_a = manager.workspace_for(&workspace_a).await.expect("A");
    let trace_a = format!("a{}", &suffix[..31]);
    let trace_b = format!("b{}", &suffix[..31]);
    let session = format!("reset-session-{suffix}");
    ws_a.ingest()
        .add_spans(vec![span(&workspace_a, &trace_a, &session)], 0)
        .await
        .expect("write A before B is built");
    wait_until_visible(&ws_a, &trace_a).await;

    let ws_b = manager.workspace_for(&workspace_b).await.expect("B");
    assert_eq!(
        visible_span_count(&ws_a, &trace_a).await,
        1,
        "building a second workspace must not wipe the lake"
    );
    ws_b.ingest()
        .add_spans(vec![span(&workspace_b, &trace_b, &session)], 0)
        .await
        .expect("write B");
    wait_until_visible(&ws_b, &trace_b).await;
    assert_eq!(visible_span_count(&ws_a, &trace_a).await, 1);
    assert_eq!(visible_span_count(&ws_b, &trace_a).await, 0);
}

#[tokio::test]
async fn shared_pool_session_count_stays_fixed_as_workspaces_grow() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let mut config = shared_config(&temp, format!("shared_pool_growth_{suffix}"));
    config.ducklake.writer_pool_size = 2;
    config.query.max_connections = 3;
    let manager = WorkspaceManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("workspace manager");
    let ids = provision_shared(&manager, &config, 4).await;
    let mut contexts = Vec::with_capacity(ids.len());
    for id in &ids {
        contexts.push(manager.workspace_for(id).await.expect("workspace"));
    }

    assert_eq!(manager.catalog_pool_count_for_test(), 1);
    assert_eq!(
        manager.catalog_session_count_for_test(),
        2 + 3 + 1,
        "four workspaces still use writer slots + query slots + one maintenance session"
    );

    let traces = write_private_spans(
        &contexts,
        &ids,
        &suffix,
        &format!("growth-session-{suffix}"),
    )
    .await;
    assert_spans_stay_private(&contexts, &traces).await;
    assert_eq!(manager.catalog_pool_count_for_test(), 1);
    assert_eq!(manager.catalog_session_count_for_test(), 2 + 3 + 1);
}

#[tokio::test]
async fn one_query_slot_serves_two_workspaces_concurrently_without_cross_read() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let mut config = shared_config(&temp, format!("shared_pool_concurrent_{suffix}"));
    config.ducklake.writer_pool_size = 1;
    config.query.max_connections = 1;
    let manager = WorkspaceManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("workspace manager");
    let ids = provision_shared(&manager, &config, 2).await;
    let contexts = vec![
        manager.workspace_for(&ids[0]).await.expect("A"),
        manager.workspace_for(&ids[1]).await.expect("B"),
    ];
    let traces = write_private_spans(
        &contexts,
        &ids,
        &suffix,
        &format!("concurrent-session-{suffix}"),
    )
    .await;

    let (a_own, a_other, b_own, b_other) = tokio::join!(
        visible_span_count(&contexts[0], &traces[0]),
        visible_span_count(&contexts[0], &traces[1]),
        visible_span_count(&contexts[1], &traces[1]),
        visible_span_count(&contexts[1], &traces[0]),
    );
    assert_eq!((a_own, a_other, b_own, b_other), (1, 0, 1, 0));
    assert_eq!(manager.catalog_pool_count_for_test(), 1);
    assert_eq!(manager.catalog_session_count_for_test(), 1 + 1 + 1);
}

#[tokio::test]
async fn isolated_workspaces_keep_separate_pools_and_separate_rows() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let mut config = shared_config(&temp, format!("iso_pool_reg_{suffix}"));
    config.ducklake.workspace_scope_mode =
        softprobe_runtime::workspace_scope::WorkspaceScopeMode::Isolated;
    config.ducklake.writer_pool_size = 1;
    config.query.max_connections = 1;
    let manager = WorkspaceManager::connect(Arc::new(config), None)
        .await
        .expect("workspace manager");
    let ids = [Uuid::new_v4().to_string(), Uuid::new_v4().to_string()];
    let schemas = [format!("iso_a_{suffix}"), format!("iso_b_{suffix}")];
    for (index, id) in ids.iter().enumerate() {
        manager
            .provision_scope(ScopeProvisioningRequest {
                scope_id: id.clone(),
                metadata_schema: schemas[index].clone(),
                data_path: temp
                    .path()
                    .join(format!("isolated-{index}"))
                    .to_string_lossy()
                    .into(),
            })
            .await
            .expect("provision isolated workspace");
    }
    let contexts = vec![
        manager.workspace_for(&ids[0]).await.expect("A"),
        manager.workspace_for(&ids[1]).await.expect("B"),
    ];

    assert_eq!(
        manager.catalog_pool_count_for_test(),
        2,
        "isolated workspaces keep a pool per physical catalog"
    );
    assert_eq!(manager.catalog_session_count_for_test(), 2 * (1 + 1 + 1));

    let traces = write_private_spans(
        &contexts,
        &ids,
        &suffix,
        &format!("isolated-session-{suffix}"),
    )
    .await;
    assert_spans_stay_private(&contexts, &traces).await;
    assert_eq!(manager.catalog_pool_count_for_test(), 2);
    assert_eq!(manager.catalog_session_count_for_test(), 2 * (1 + 1 + 1));
}
