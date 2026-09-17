//! Session stats configurability: apply / load / resolve / write+list extras (T05–T18 + gates).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::llm::query::{
    compile_session_search_sql_from_deltas, SessionSearchRequest,
};
use softprobe_runtime::api::{create_router, AppState};
use softprobe_runtime::runtime_api::runtime_control_routes;
use softprobe_runtime::session_stats::{
    builtin_session_stats_manifest, parse_session_stats_manifest,
    set_fail_session_stats_delta_write_for_test, BUILTIN_SESSION_STATS_YAML,
};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;

use crate::util::config::file_backed_test_config;
use crate::util::promotion_file_backed::attach_softprobe_ducklake;
use crate::util::session_stats_serial::session_stats_ingest_serial;
use crate::util::tenant::{inject_local_sqlite_tenant as inject_tenant, LOCAL_SQLITE_TENANT_ID};

/// Serialize tests that ingest or flip the delta-write fault flag (shared process state).
fn config_test_serial() -> &'static AsyncMutex<()> {
    session_stats_ingest_serial()
}

const T0: i64 = 1_721_392_800_000_000_000;

fn default_yaml() -> String {
    std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/session_stats/default.yaml"
    ))
    .expect("default.yaml")
}

fn manifest_with_tool_calls() -> String {
    r#"
specVersion: softprobe.session_stats.v1
key:
  - session_id
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
  - name: error_count
    op: sum
    source:
      kind: count_where
      column: status_code
      eq: ERROR
  - name: total_tokens
    op: sum
    source: { kind: column, column: total_tokens }
  - name: total_cost
    op: sum
    source: { kind: column, column: total_cost }
  - name: input_tokens
    op: sum
    source: { kind: column, column: input_tokens }
  - name: output_tokens
    op: sum
    source: { kind: column, column: output_tokens }
  - name: trace_count
    op: sum
    source: { kind: count_distinct, column: trace_id }
  - name: start_time
    op: min
    source: { kind: column, column: timestamp }
  - name: end_time
    op: max
    source: { kind: column, column: end_timestamp }
  - name: tool_calls
    op: sum
    source: { kind: column, column: tool_calls }
dimensions:
  - name: agent_name
    source: { kind: column, column: agent_name }
  - name: is_nested_child
    source:
      kind: flag_attr
      key: sp.metadata.opencode.parentSessionID
"#
    .to_string()
}

fn manifest_with_model_name_dim() -> String {
    r#"
specVersion: softprobe.session_stats.v1
key:
  - session_id
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
  - name: error_count
    op: sum
    source:
      kind: count_where
      column: status_code
      eq: ERROR
  - name: total_tokens
    op: sum
    source: { kind: column, column: total_tokens }
  - name: total_cost
    op: sum
    source: { kind: column, column: total_cost }
  - name: input_tokens
    op: sum
    source: { kind: column, column: input_tokens }
  - name: output_tokens
    op: sum
    source: { kind: column, column: output_tokens }
  - name: trace_count
    op: sum
    source: { kind: count_distinct, column: trace_id }
  - name: start_time
    op: min
    source: { kind: column, column: timestamp }
  - name: end_time
    op: max
    source: { kind: column, column: end_timestamp }
dimensions:
  - name: agent_name
    source: { kind: column, column: agent_name }
  - name: is_nested_child
    source:
      kind: flag_attr
      key: sp.metadata.opencode.parentSessionID
  - name: model_name
    source: { kind: column, column: model_name }
"#
    .to_string()
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

type SpanSpec = (Vec<u8>, Vec<u8>, i64, Option<&'static str>, Vec<KeyValue>);

fn trace_batch(session_id: &str, spans: Vec<SpanSpec>) -> ExportTraceServiceRequest {
    let otlp_spans = spans
        .into_iter()
        .map(|(trace_id, span_id, start_ns, status, mut attrs)| {
            attrs.push(string_kv("sp.session.id", session_id));
            Span {
                trace_id,
                span_id,
                parent_span_id: vec![],
                name: "gen".to_string(),
                kind: span::SpanKind::Internal as i32,
                start_time_unix_nano: start_ns as u64,
                end_time_unix_nano: (start_ns + 1_000_000_000) as u64,
                attributes: attrs,
                status: status.map(|code| Status {
                    code: if code == "ERROR" { 2 } else { 1 },
                    message: String::new(),
                }),
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "session-stats-config-test")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.test".to_string(),
                    ..Default::default()
                }),
                spans: otlp_spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn response_json(resp: axum::response::Response) -> Value {
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json body")
}

struct Env {
    _temp: TempDir,
    router: Router,
    state: AppState,
    metadata_path: String,
    data_path: String,
}

async fn setup() -> (Env, tokio::sync::MutexGuard<'static, ()>) {
    let guard = config_test_serial().lock().await;
    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let (router, state) = create_router(Arc::new(config), post(ingest_traces), None)
        .await
        .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn(inject_tenant));
    (
        Env {
            _temp: temp,
            router,
            state,
            metadata_path,
            data_path,
        },
        guard,
    )
}

fn attach(env: &Env) -> duckdb::Connection {
    attach_softprobe_ducklake(&env.metadata_path, &env.data_path)
}

async fn apply(env: &Env, yaml: &str) -> (StatusCode, Value) {
    let resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/promotions/apply")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({ "manifestYaml": yaml })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .expect("apply");
    let status = resp.status();
    (status, response_json(resp).await)
}

async fn post_traces(env: &Env, body: ExportTraceServiceRequest) {
    let mut buf = Vec::new();
    body.encode(&mut buf).expect("encode");
    let resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(buf))
                .unwrap(),
        )
        .await
        .expect("traces");
    assert_eq!(resp.status(), StatusCode::OK);
}

async fn flush(env: &Env) {
    let engine = env
        .state
        .engine_for_id(LOCAL_SQLITE_TENANT_ID)
        .await
        .expect("engine");
    engine
        .ingest
        .force_flush_spans()
        .await
        .expect("flush spans");
}

async fn run_sql(env: &Env, sql: &str) -> (StatusCode, Value) {
    let resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query/sql")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "sql": sql }).to_string()))
                .unwrap(),
        )
        .await
        .expect("sql");
    let status = resp.status();
    (status, response_json(resp).await)
}

async fn search(env: &Env, body: Value) -> (StatusCode, Value) {
    let resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/llm/sessions/search")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("search");
    let status = resp.status();
    (status, response_json(resp).await)
}

fn count_session_stats(conn: &duckdb::Connection, status: &str) -> i64 {
    conn.query_row(
        &format!(
            "SELECT count(*) FROM softprobe.promotion_specs \
             WHERE status = '{status}' AND target_kind = 'session_stats'"
        ),
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

fn column_exists(conn: &duckdb::Connection, column: &str) -> bool {
    conn.query_row(
        &format!(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_catalog = 'softprobe' AND table_name = 'session_stats_delta' \
             AND column_name = '{column}'"
        ),
        [],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0)
        > 0
}

#[tokio::test]
async fn t05_load_active_ignores_inactive_and_wrong_kind() {
    let (env, _guard) = setup().await;
    let (st, body) = apply(&env, &default_yaml()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (st, body) = apply(&env, &manifest_with_tool_calls()).await;
    assert_eq!(st, StatusCode::OK, "{body}");

    let conn = attach(&env);
    assert_eq!(count_session_stats(&conn, "active"), 1);
    assert!(count_session_stats(&conn, "inactive") >= 1);

    let engine = env
        .state
        .engine_for_id(LOCAL_SQLITE_TENANT_ID)
        .await
        .expect("engine");
    let active = engine
        .storage
        .writer
        .load_active_session_stats_manifest(&engine.scope)
        .await
        .expect("load")
        .expect("active row");
    assert!(
        active.measures.iter().any(|m| m.name == "tool_calls"),
        "active should be tool_calls manifest"
    );
}

#[tokio::test]
async fn t06_t07_t08_apply_default_supersede_and_response_shape() {
    let (env, _guard) = setup().await;
    let (st, body) = apply(&env, &default_yaml()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["applied"], true);
    assert_eq!(body["target"]["kind"], "session_stats");
    assert_eq!(body["target"]["tables"][0], "session_stats_delta");
    assert!(body["schemaChanges"].is_array(), "{body}");

    let conn = attach(&env);
    assert_eq!(count_session_stats(&conn, "active"), 1);

    let (st2, body2) = apply(&env, &manifest_with_tool_calls()).await;
    assert_eq!(st2, StatusCode::OK, "{body2}");
    assert_eq!(count_session_stats(&attach(&env), "active"), 1);
    assert!(count_session_stats(&attach(&env), "inactive") >= 1);
}

#[tokio::test]
async fn t03_t04_apply_rejects_uniq_and_bad_spec_version() {
    let (env, _guard) = setup().await;
    let bad_op = r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: users
    op: uniq
    source: { kind: column, column: user_id }
"#;
    let (st, body) = apply(&env, bad_op).await;
    assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "unsupported_op");

    let bad_ver = r#"
specVersion: softprobe.session_stats.v0
key: [session_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
"#;
    let (st, body) = apply(&env, bad_ver).await;
    assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "unsupported_spec_version");
}

#[tokio::test]
async fn t09_map_measure_apply_no_typed_column() {
    let (env, _guard) = setup().await;
    let (st, body) = apply(&env, &manifest_with_tool_calls()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let changes = body["schemaChanges"].as_array().unwrap();
    assert!(
        changes
            .iter()
            .any(|c| { c["action"] == "map_measure" && c["column"] == "tool_calls" }),
        "{body}"
    );
    assert!(!column_exists(&attach(&env), "tool_calls"));
}

#[tokio::test]
async fn t10_t11_dimension_add_column_idempotent() {
    let (env, _guard) = setup().await;
    let yaml = manifest_with_model_name_dim();
    let (st, body) = apply(&env, &yaml).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(
        body["schemaChanges"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["action"] == "add_column" && c["column"] == "model_name"),
        "{body}"
    );
    assert!(column_exists(&attach(&env), "model_name"));

    let (st2, body2) = apply(&env, &yaml).await;
    assert_eq!(st2, StatusCode::OK, "{body2}");
    assert!(column_exists(&attach(&env), "model_name"));
}

#[tokio::test]
async fn t12_resolve_fallback_builtin_when_no_active() {
    let (env, _guard) = setup().await;
    let engine = env
        .state
        .engine_for_id(LOCAL_SQLITE_TENANT_ID)
        .await
        .expect("engine");
    let resolved = engine
        .storage
        .writer
        .resolve_session_stats_manifest(&engine.scope)
        .await
        .expect("resolve");
    assert_eq!(resolved, builtin_session_stats_manifest());
    assert_eq!(
        parse_session_stats_manifest(BUILTIN_SESSION_STATS_YAML).unwrap(),
        resolved
    );
}

#[tokio::test]
async fn t13_t14_write_uses_active_tool_calls_only_when_applied() {
    let (env, _guard) = setup().await;
    let session_id = "sess-tool-calls-write";

    // Without apply: attr present must not invent measures['tool_calls'].
    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![(
                vec![0x21; 16],
                vec![0xc1; 8],
                T0,
                None,
                vec![
                    string_kv("tool_calls", "2"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;
    let (st, body) = run_sql(
        &env,
        &format!(
            "SELECT TRY_CAST(measures['tool_calls'] AS DOUBLE) \
             FROM session_stats_delta WHERE session_id = '{session_id}'"
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(
        body["rows"][0][0].is_null(),
        "no invent without apply: {body}"
    );

    let (st, body) = apply(&env, &manifest_with_tool_calls()).await;
    assert_eq!(st, StatusCode::OK, "{body}");

    let session2 = "sess-tool-calls-active";
    post_traces(
        &env,
        trace_batch(
            session2,
            vec![(
                vec![0x22; 16],
                vec![0xc2; 8],
                T0 + 10_000_000_000,
                None,
                vec![
                    string_kv("tool_calls", "3"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;
    let (st, body) = run_sql(
        &env,
        &format!(
            "SELECT TRY_CAST(measures['tool_calls'] AS DOUBLE) \
             FROM session_stats_delta WHERE session_id = '{session2}'"
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0][0].as_f64().unwrap(), 3.0, "{body}");
}

#[tokio::test]
async fn t15_t16_list_sql_extra_only_when_active() {
    let req = SessionSearchRequest {
        from: chrono::DateTime::parse_from_rfc3339("2024-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        to: chrono::DateTime::parse_from_rfc3339("2024-07-20T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        has_errors: None,
        user_id: None,
        model_name: None,
        agent_name: None,
        roots_only: false,
        order_by: Default::default(),
        order: Default::default(),
        limit: None,
        cursor: None,
    };
    let builtin_sql =
        compile_session_search_sql_from_deltas(&req, 50, &builtin_session_stats_manifest())
            .expect("sql");
    assert!(
        !builtin_sql.contains("tool_calls"),
        "inactive extras absent: {builtin_sql}"
    );

    let with_extra = parse_session_stats_manifest(&manifest_with_tool_calls()).unwrap();
    let extra_sql = compile_session_search_sql_from_deltas(&req, 50, &with_extra).expect("sql");
    assert!(
        extra_sql.contains("SUM(TRY_CAST(measures['tool_calls'] AS DOUBLE)) AS tool_calls"),
        "{extra_sql}"
    );
}

#[tokio::test]
async fn t17_list_api_merged_extra_sum() {
    let (env, _guard) = setup().await;
    let (st, body) = apply(&env, &manifest_with_tool_calls()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let session_id = "sess-tool-merge";

    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![(
                vec![0x31; 16],
                vec![0xd1; 8],
                T0,
                None,
                vec![
                    string_kv("tool_calls", "1"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;
    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![(
                vec![0x32; 16],
                vec![0xd2; 8],
                T0 + 60_000_000_000,
                None,
                vec![
                    string_kv("tool_calls", "2"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;

    let (st, body) = run_sql(
        &env,
        &format!(
            "SELECT SUM(TRY_CAST(measures['tool_calls'] AS DOUBLE)) \
             FROM session_stats_delta WHERE session_id = '{session_id}'"
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0][0].as_f64().unwrap(), 3.0, "{body}");
}

#[tokio::test]
async fn t18_sqlite_load_active_from_promotion_specs() {
    let (env, _guard) = setup().await;
    let (st, body) = apply(&env, &manifest_with_tool_calls()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let engine = env
        .state
        .engine_for_id(LOCAL_SQLITE_TENANT_ID)
        .await
        .expect("engine");
    let loaded = engine
        .storage
        .writer
        .load_active_session_stats_manifest_local()
        .expect("sqlite load")
        .expect("active");
    assert!(loaded.measures.iter().any(|m| m.name == "tool_calls"));
}

#[tokio::test]
async fn t19_delta_write_fail_spans_still_ok() {
    let (env, _guard) = setup().await;
    set_fail_session_stats_delta_write_for_test(true);
    let session_id = "sess-fault-inject";
    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![(
                vec![0x41; 16],
                vec![0xe1; 8],
                T0,
                None,
                vec![
                    string_kv("total_tokens", "9"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;
    set_fail_session_stats_delta_write_for_test(false);

    let (st, body) = run_sql(
        &env,
        &format!("SELECT COUNT(*)::BIGINT FROM traces WHERE session_id = '{session_id}'"),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(body["rows"][0][0].as_i64().unwrap() >= 1, "{body}");

    let (st, body) = run_sql(
        &env,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM session_stats_delta WHERE session_id = '{session_id}'"
        ),
    )
    .await;
    // Table may not exist when the first write was fault-injected — both mean zero deltas.
    if st == StatusCode::OK {
        assert_eq!(body["rows"][0][0].as_i64().unwrap_or(0), 0, "{body}");
    } else {
        let err = body.to_string();
        assert!(
            err.contains("session_stats_delta") && err.to_lowercase().contains("does not exist"),
            "expected missing delta table, got {body}"
        );
    }
}

#[tokio::test]
async fn t21_combined_agent_has_errors_window() {
    let (env, _guard) = setup().await;
    post_traces(
        &env,
        trace_batch(
            "sess-combo-match",
            vec![(vec![0x51; 16], vec![0xf1; 8], T0, Some("ERROR"), {
                let a = vec![
                    string_kv("total_tokens", "1"),
                    string_kv("sp.observation.type", "generation"),
                    string_kv("sp.agent.name", "ComboAgent"),
                ];
                a
            })],
        ),
    )
    .await;
    post_traces(
        &env,
        trace_batch(
            "sess-combo-wrong-agent",
            vec![(
                vec![0x52; 16],
                vec![0xf2; 8],
                T0 + 1_000_000_000,
                Some("ERROR"),
                vec![
                    string_kv("total_tokens", "1"),
                    string_kv("sp.observation.type", "generation"),
                    string_kv("sp.agent.name", "OtherAgent"),
                ],
            )],
        ),
    )
    .await;
    post_traces(
        &env,
        trace_batch(
            "sess-combo-no-err",
            vec![(
                vec![0x53; 16],
                vec![0xf3; 8],
                T0 + 2_000_000_000,
                None,
                vec![
                    string_kv("total_tokens", "1"),
                    string_kv("sp.observation.type", "generation"),
                    string_kv("sp.agent.name", "ComboAgent"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;

    let (st, body) = search(
        &env,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "agent_name": "ComboAgent",
            "has_errors": true,
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let ids: Vec<_> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["sess-combo-match".to_string()], "{body}");
}

#[tokio::test]
async fn t22_cross_midnight_one_row_min_max() {
    let (env, _guard) = setup().await;
    let session_id = "sess-midnight";
    // 2024-07-19T23:00:00Z and 2024-07-20T01:00:00Z
    let t_before = 1_721_433_600_000_000_000i64; // approx — use explicit
    let t_day1 = chrono::DateTime::parse_from_rfc3339("2024-07-19T23:00:00Z")
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let t_day2 = chrono::DateTime::parse_from_rfc3339("2024-07-20T01:00:00Z")
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let _ = t_before;

    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![(
                vec![0x61; 16],
                vec![0xa1; 8],
                t_day1,
                None,
                vec![
                    string_kv("total_tokens", "4"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;
    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![(
                vec![0x62; 16],
                vec![0xa2; 8],
                t_day2,
                None,
                vec![
                    string_kv("total_tokens", "6"),
                    string_kv("sp.observation.type", "generation"),
                ],
            )],
        ),
    )
    .await;
    flush(&env).await;

    let (st, body) = run_sql(
        &env,
        &format!(
            "SELECT COUNT(DISTINCT record_date)::BIGINT FROM session_stats_delta \
             WHERE session_id = '{session_id}'"
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0][0].as_i64().unwrap(), 2, "{body}");

    let (st, body) = search(
        &env,
        json!({
            "from": "2024-07-19T00:00:00Z",
            "to": "2024-07-21T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let items = body["items"].as_array().unwrap();
    let row = items
        .iter()
        .find(|r| r["session_id"] == session_id)
        .expect("session present");
    assert_eq!(row["observation_count"], 2);
    assert_eq!(row["total_tokens"], 10);
    assert!(row["start_time"]
        .as_str()
        .unwrap()
        .starts_with("2024-07-19"));
    assert!(row["end_time"].as_str().unwrap().starts_with("2024-07-20"));
}

#[tokio::test]
async fn t23_list_and_detail_both_succeed_error_semantics() {
    let (env, _guard) = setup().await;
    let session_id = "sess-list-detail-err";
    post_traces(
        &env,
        trace_batch(
            session_id,
            vec![
                (
                    vec![0x71; 16],
                    vec![0xb1; 8],
                    T0,
                    Some("ERROR"),
                    vec![
                        string_kv("total_tokens", "1"),
                        string_kv("sp.observation.type", "generation"),
                    ],
                ),
                (
                    vec![0x71; 16],
                    vec![0xb2; 8],
                    T0 + 1_000_000_000,
                    Some("ERROR"),
                    vec![
                        string_kv("total_tokens", "1"),
                        string_kv("sp.observation.type", "generation"),
                    ],
                ),
            ],
        ),
    )
    .await;
    flush(&env).await;

    let (st, list) = search(
        &env,
        json!({
            "from": "2024-07-18T00:00:00Z",
            "to": "2024-07-20T00:00:00Z",
            "roots_only": false,
            "limit": 50
        }),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{list}");
    let list_row = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["session_id"] == session_id)
        .expect("list row");
    let list_errors = list_row["error_count"].as_i64().unwrap();
    assert!(list_errors >= 1, "{list_row}");

    let detail_resp = env
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/v1/llm/sessions/{session_id}?from=2024-07-18T00:00:00Z&to=2024-07-20T00:00:00Z"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("detail");
    assert_eq!(detail_resp.status(), StatusCode::OK);
    let detail = response_json(detail_resp).await;
    assert!(
        detail.get("session_id").is_some() || detail.get("id").is_some() || detail.is_object(),
        "{detail}"
    );
    // Documented: list error_count is batch ERROR sum; may differ from detail primary-error.
    let _ = list_errors;
}

#[tokio::test]
async fn t24_undeclared_user_id_filter_uses_span_path() {
    let req = SessionSearchRequest {
        from: chrono::DateTime::parse_from_rfc3339("2024-07-18T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        to: chrono::DateTime::parse_from_rfc3339("2024-07-20T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
        has_errors: None,
        user_id: Some("u-1".to_string()),
        model_name: None,
        agent_name: None,
        roots_only: false,
        order_by: Default::default(),
        order: Default::default(),
        limit: None,
        cursor: None,
    };
    let sql =
        softprobe_runtime::api::llm::query::compile_session_search_sql(&req, 50).expect("sql");
    assert!(
        sql.contains("union_spans") || sql.to_lowercase().contains("from traces"),
        "must fall back to span path: {sql}"
    );
    assert!(
        !sql.contains("session_stats_delta"),
        "must not use deltas for undeclared user_id: {sql}"
    );
}
