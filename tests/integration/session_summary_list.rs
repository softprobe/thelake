//! Stage 3 HTTP e2e: ingest → dirty → `reduce_tenant` → `sessions/search`.
//!
//! Covers every list filter against rows the reducer wrote (not hand-seeded
//! SQL). Summary path must not return spans/details; detail still reads lake.
//!
//! Requires ducklake-postgres (`make setup` / `make test-e2e`).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use softprobe_runtime::api::AppState;
use softprobe_runtime::config::Config;
use softprobe_runtime::models::attr_keys::{gen_ai, resource, sp};
use softprobe_runtime::session_summary::{ensure_session_summary_tables, reduce_tenant};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::util::otlp::{double_kv, int_kv, string_kv};

fn postgres_summary_config(temp: &TempDir, metadata_schema: String) -> Config {
    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.shrink_pools_for_tests();
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into());

    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.catalog_alias = "softprobe".to_string();
    config.ducklake.metadata_schema = metadata_schema;
    config.ducklake.data_path = temp.path().join("data").to_string_lossy().into();
    config.ducklake.data_inlining_row_limit = Some(0);

    config.ingest.flush_interval_seconds = 2;
    config.session_summary.enabled = true;
    // Fixture spans are recent; keep default clamp. Historical fixed epochs
    // would fall outside max_reduce_span_seconds (clamped to now).
    config
}

async fn pg_reachable() -> bool {
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio_postgres::connect(
                "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
                tokio_postgres::NoTls,
            ),
        )
        .await,
        Ok(Ok(_))
    )
}

async fn build_summary_router(
    metadata_schema: String,
) -> Option<(Router, AppState, TempDir, String)> {
    if !pg_reachable().await {
        return None;
    }
    let temp = TempDir::new().expect("tempdir");
    let config = Arc::new(postgres_summary_config(&temp, metadata_schema.clone()));
    let (router, state) = softprobe_runtime::api::create_router(
        config,
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router");

    let registry = state.engines.scope_registry().expect("postgres registry");
    let client = registry.pool().get().await.expect("pg client");
    ensure_session_summary_tables(&client, &metadata_schema)
        .await
        .expect("ensure summary ddl");
    let q = format!("\"{}\"", metadata_schema.replace('"', "\"\""));
    let _ = client
        .execute(
            &format!("TRUNCATE {q}.session_summary, {q}.session_summary_dirty"),
            &[],
        )
        .await;

    Some((router, state, temp, metadata_schema))
}

async fn response_json(resp: axum::response::Response<Body>) -> Value {
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    serde_json::from_slice(&body).expect("json body")
}

async fn ingest(router: &Router, req: ExportTraceServiceRequest) {
    let mut buf = Vec::new();
    req.encode(&mut buf).unwrap();
    let http = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(buf))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(http).await.unwrap().status(),
        StatusCode::OK
    );
}

async fn flush(state: &AppState) {
    state
        .engine_for_id("")
        .await
        .expect("engine")
        .ingest
        .force_flush_spans()
        .await
        .expect("flush");
}

/// Run the same pipeline the leased job runs (claim dirty → lake agg → UPSERT).
async fn run_reduce(state: &AppState) -> usize {
    let engine = state.engine_for_id("").await.expect("engine");
    let registry = state.engines.scope_registry().expect("registry");
    let mut dk = state.engines.config().ducklake.clone();
    dk.metadata_schema = engine.scope.metadata_schema.clone();
    dk.data_path = engine.scope.data_path.clone();
    let cfg = &state.engines.config().session_summary;
    reduce_tenant(
        registry.pool(),
        &dk.metadata_schema,
        "",
        &dk,
        cfg.max_sessions_per_reduce,
        cfg.max_reduce_span_seconds,
    )
    .await
    .expect("reduce_tenant")
}

async fn dirty_count(state: &AppState, schema: &str) -> i64 {
    let registry = state.engines.scope_registry().expect("registry");
    let client = registry.pool().get().await.expect("client");
    let q = format!("\"{}\"", schema.replace('"', "\"\""));
    client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {q}.session_summary_dirty"),
            &[],
        )
        .await
        .expect("dirty count")
        .get(0)
}

async fn search(router: &Router, body: Value) -> Value {
    let (status, v) = search_raw(router, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body} → {v}");
    v
}

async fn search_raw(router: &Router, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/llm/sessions/search")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("search");
    let status = resp.status();
    (status, response_json(resp).await)
}

fn session_ids(v: &Value) -> Vec<&str> {
    v["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["session_id"].as_str().unwrap())
        .collect()
}

fn assert_summary_only_item(item: &Value) {
    assert!(item.get("session_id").is_some());
    assert!(item.get("observation_count").is_some());
    assert!(item.get("error_count").is_some());
    assert!(item.get("traces").is_none(), "list must not return traces");
    assert!(item.get("scores").is_none(), "list must not return scores");
    assert!(
        item.get("observations").is_none(),
        "list must not return observations"
    );
    assert!(item.get("spans").is_none(), "list must not return spans");
}

/// Search window covering the recent fixture (and excluding sess-old).
fn window() -> Value {
    let now = Utc::now();
    json!({
        "from": (now - ChronoDuration::hours(2)).to_rfc3339(),
        "to": (now + ChronoDuration::minutes(5)).to_rfc3339(),
        "order_by": "start_time",
        "order": "desc",
        "limit": 50
    })
}

struct SpanSpec {
    session_id: &'static str,
    trace: u8,
    /// Seconds before `now` for start (larger = older).
    start_ago_s: i64,
    duration_s: i64,
    error: bool,
    agent: &'static str,
    user: &'static str,
    model: &'static str,
    tokens: i64,
    cost: f64,
}

fn llm_span(spec: &SpanSpec) -> ExportTraceServiceRequest {
    let end = Utc::now() - ChronoDuration::seconds(spec.start_ago_s);
    let start = end - ChronoDuration::seconds(spec.duration_s);
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv(resource::SERVICE_NAME, "llm-gateway")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "softprobe.llm".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: vec![spec.trace; 16],
                    span_id: vec![spec.trace.wrapping_add(0x40); 8],
                    parent_span_id: vec![],
                    name: "chat.completions".to_string(),
                    kind: span::SpanKind::Client as i32,
                    start_time_unix_nano: start.timestamp_nanos_opt().unwrap() as u64,
                    end_time_unix_nano: end.timestamp_nanos_opt().unwrap() as u64,
                    attributes: vec![
                        string_kv(sp::SESSION_ID, spec.session_id),
                        string_kv(sp::OBSERVATION_TYPE, "generation"),
                        string_kv(sp::AGENT_NAME, spec.agent),
                        string_kv(sp::USER_ID, spec.user),
                        string_kv(gen_ai::REQUEST_MODEL, spec.model),
                        string_kv(gen_ai::PROVIDER_NAME, "test"),
                        int_kv(gen_ai::USAGE_INPUT_TOKENS, spec.tokens / 2),
                        int_kv(gen_ai::USAGE_OUTPUT_TOKENS, spec.tokens / 2),
                        int_kv(gen_ai::USAGE_TOTAL_TOKENS, spec.tokens),
                        double_kv(sp::COST_TOTAL, spec.cost),
                    ],
                    status: Some(Status {
                        code: if spec.error { 2 } else { 1 },
                        message: if spec.error {
                            "boom".into()
                        } else {
                            String::new()
                        },
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// In-window sessions for filter matrix; sess-old is far outside `window()`.
fn filter_fixture() -> [SpanSpec; 4] {
    [
        SpanSpec {
            session_id: "sess-ok",
            trace: 0xa1,
            start_ago_s: 300,
            duration_s: 50,
            error: false,
            agent: "agent-a",
            user: "u1",
            model: "gpt-4o",
            tokens: 100,
            cost: 0.1,
        },
        SpanSpec {
            session_id: "sess-err",
            trace: 0xa2,
            start_ago_s: 200,
            duration_s: 100,
            error: true,
            agent: "agent-b",
            user: "u2",
            model: "claude",
            tokens: 200,
            cost: 0.2,
        },
        SpanSpec {
            session_id: "sess-mix",
            trace: 0xa3,
            start_ago_s: 100,
            duration_s: 50,
            error: true,
            agent: "agent-a",
            user: "u1",
            model: "claude",
            tokens: 50,
            cost: 0.05,
        },
        SpanSpec {
            session_id: "sess-old",
            trace: 0xa4,
            // Outside the 2h search window but same UTC day as the others so a
            // coalesced DuckLake flush (same record_date) still succeeds.
            start_ago_s: 3 * 3600,
            duration_s: 10,
            error: true,
            agent: "agent-a",
            user: "u1",
            model: "gpt-4o",
            tokens: 999,
            cost: 9.0,
        },
    ]
}

async fn ingest_filter_fixture(router: &Router) {
    for (i, spec) in filter_fixture().iter().enumerate() {
        let mut req = llm_span(spec);
        let span = &mut req.resource_spans[0].scope_spans[0].spans[0];
        span.span_id = vec![0xb0 + i as u8; 8];
        ingest(router, req).await;

        // Reduce resolves agent_name from typed `agent_name` or an agent span's
        // message_type (not attributes['sp.agent.name']). Emit an agent span
        // without usage attrs so token SUMs stay generation-only.
        let mut agent = llm_span(spec);
        {
            let span = &mut agent.resource_spans[0].scope_spans[0].spans[0];
            span.name = spec.agent.to_string();
            span.span_id = vec![0xc0 + i as u8; 8];
            span.attributes.retain(|kv| {
                kv.key == sp::SESSION_ID
                    || kv.key == sp::OBSERVATION_TYPE
                    || kv.key == sp::AGENT_NAME
            });
            for kv in &mut span.attributes {
                if kv.key == sp::OBSERVATION_TYPE {
                    *kv = string_kv(sp::OBSERVATION_TYPE, "agent");
                }
            }
        }
        ingest(router, agent).await;

        // sess-err gets a second ERROR generation so error_count=2.
        if spec.session_id == "sess-err" {
            let mut extra = llm_span(spec);
            let span = &mut extra.resource_spans[0].scope_spans[0].spans[0];
            span.span_id = vec![0xee; 8];
            span.trace_id = vec![0xa2; 16];
            span.trace_id[0] = 0x02;
            let end = Utc::now() - ChronoDuration::seconds(spec.start_ago_s - 10);
            let start = end - ChronoDuration::seconds(5);
            span.start_time_unix_nano = start.timestamp_nanos_opt().unwrap() as u64;
            span.end_time_unix_nano = end.timestamp_nanos_opt().unwrap() as u64;
            ingest(router, extra).await;
        }
    }
}

#[tokio::test]
async fn http_session_summary_empty_before_reduce_ignores_lake() {
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_empty_{suffix}");
    let Some((router, state, _temp, schema)) = build_summary_router(schema).await else {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    };

    ingest(&router, llm_span(&filter_fixture()[0])).await;
    flush(&state).await;
    assert!(
        dirty_count(&state, &schema).await >= 1,
        "flush must dirty the session before reduce"
    );

    let v = search(&router, window()).await;
    assert!(
        v["items"].as_array().unwrap().is_empty(),
        "pre-reduce must not fall back to lake: {v}"
    );
}

#[tokio::test]
async fn http_session_summary_enabled_false_still_no_lake_fallback() {
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_dis_{suffix}");
    if !pg_reachable().await {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    }
    let temp = TempDir::new().expect("tempdir");
    let mut config = postgres_summary_config(&temp, schema.clone());
    config.session_summary.enabled = false;
    let config = Arc::new(config);
    let (router, state) = softprobe_runtime::api::create_router(
        config,
        axum::routing::post(softprobe_runtime::api::ingestion::traces::ingest_traces),
        None,
    )
    .await
    .expect("router");

    let registry = state.engines.scope_registry().expect("postgres registry");
    let client = registry.pool().get().await.expect("pg client");
    ensure_session_summary_tables(&client, &schema)
        .await
        .expect("ensure summary ddl");
    let q = format!("\"{}\"", schema.replace('"', "\"\""));
    let _ = client
        .execute(
            &format!("TRUNCATE {q}.session_summary, {q}.session_summary_dirty"),
            &[],
        )
        .await;

    ingest(&router, llm_span(&filter_fixture()[0])).await;
    flush(&state).await;
    assert_eq!(
        dirty_count(&state, &schema).await,
        0,
        "enabled=false must not dirty"
    );

    let v = search(&router, window()).await;
    assert!(
        v["items"].as_array().unwrap().is_empty(),
        "enabled=false must still read session_summary (empty), never lake: {v}"
    );
}

#[tokio::test]
async fn http_ingest_reduce_list_every_filter() {
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_filt_{suffix}");
    let Some((router, state, _temp, schema)) = build_summary_router(schema).await else {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    };

    ingest_filter_fixture(&router).await;
    flush(&state).await;
    let dirty_n = dirty_count(&state, &schema).await;
    assert!(
        dirty_n >= 3,
        "expected dirty rows for in-window sessions, got {dirty_n}"
    );

    let reduced = run_reduce(&state).await;
    assert!(
        reduced >= 3,
        "reduce must write in-window sessions, got {reduced} (dirty was {dirty_n})"
    );

    let all = search(&router, window()).await;
    assert_eq!(
        session_ids(&all),
        vec!["sess-mix", "sess-err", "sess-ok"],
        "time range must exclude sess-old; got {all}"
    );
    for item in all["items"].as_array().unwrap() {
        assert_summary_only_item(item);
    }

    let ok = all["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["session_id"] == "sess-ok")
        .expect("sess-ok");
    assert_eq!(ok["error_count"], 0);
    assert_eq!(ok["agent_name"], "agent-a");
    assert_eq!(ok["user_ids"], json!(["u1"]));
    assert_eq!(ok["models"], json!(["gpt-4o"]));
    assert_eq!(ok["total_tokens"], 100);
    assert!((ok["total_cost"].as_f64().unwrap() - 0.1).abs() < 1e-9);
    assert!(ok["observation_count"].as_i64().unwrap() >= 1);

    let mut body = window();
    body["has_errors"] = json!(true);
    assert_eq!(
        session_ids(&search(&router, body).await),
        vec!["sess-mix", "sess-err"]
    );

    let mut body = window();
    body["has_errors"] = json!(false);
    assert_eq!(session_ids(&search(&router, body).await), vec!["sess-ok"]);

    let mut body = window();
    body["agent_name"] = json!("agent-a");
    assert_eq!(
        session_ids(&search(&router, body).await),
        vec!["sess-mix", "sess-ok"]
    );

    let mut body = window();
    body["user_id"] = json!("u2");
    assert_eq!(session_ids(&search(&router, body).await), vec!["sess-err"]);

    let mut body = window();
    body["model_name"] = json!("claude");
    assert_eq!(
        session_ids(&search(&router, body).await),
        vec!["sess-mix", "sess-err"]
    );

    let mut body = window();
    body["agent_name"] = json!("agent-a");
    body["user_id"] = json!("u1");
    body["model_name"] = json!("claude");
    body["has_errors"] = json!(true);
    let hit = search(&router, body).await;
    assert_eq!(session_ids(&hit), vec!["sess-mix"]);
    assert_summary_only_item(&hit["items"][0]);

    let mut body = window();
    body["limit"] = json!(1);
    let p1 = search(&router, body.clone()).await;
    assert_eq!(session_ids(&p1), vec!["sess-mix"]);
    body["cursor"] = json!(p1["next_cursor"].as_str().expect("next_cursor"));
    assert_eq!(session_ids(&search(&router, body).await), vec!["sess-err"]);

    // sess-err wins: error_count=2, tokens=200, cost=0.2, duration=100s.
    for (order_by, expect_first) in [
        ("error_count", "sess-err"),
        ("total_tokens", "sess-err"),
        ("total_cost", "sess-err"),
        ("duration", "sess-err"),
    ] {
        let mut body = window();
        body["order_by"] = json!(order_by);
        body["order"] = json!("desc");
        let v = search(&router, body).await;
        assert_eq!(v["cursor_supported"], false);
        assert_eq!(
            v["items"][0]["session_id"], expect_first,
            "order_by={order_by}: {v}"
        );
    }
}

#[tokio::test]
async fn http_session_detail_still_reads_lake_after_summary_reduce() {
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_detail_{suffix}");
    let Some((router, state, _temp, schema)) = build_summary_router(schema).await else {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    };

    let spec = SpanSpec {
        session_id: "detail-sess",
        trace: 0xd1,
        start_ago_s: 60,
        duration_s: 50,
        error: false,
        agent: "agent-a",
        user: "u1",
        model: "gpt-4o",
        tokens: 100,
        cost: 0.1,
    };
    ingest(&router, llm_span(&spec)).await;
    flush(&state).await;
    assert!(dirty_count(&state, &schema).await >= 1);
    assert!(run_reduce(&state).await >= 1);

    let list = search(&router, window()).await;
    assert!(
        session_ids(&list).contains(&"detail-sess"),
        "summary list missing detail-sess: {list}"
    );
    assert_summary_only_item(&list["items"][0]);

    let from = (Utc::now() - ChronoDuration::hours(2))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let to = (Utc::now() + ChronoDuration::minutes(5))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    // Encode `:` so http::Uri accepts the query string.
    let detail = Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/llm/sessions/detail-sess?from={}&to={}",
            from.replace(':', "%3A"),
            to.replace(':', "%3A"),
        ))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(detail).await.expect("detail");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(body["session_id"], "detail-sess");
    assert!(
        body["traces"].as_array().unwrap().len() >= 1,
        "detail must still return traces from lake: {body}"
    );
}

#[tokio::test]
async fn http_session_summary_invalid_cursor_and_order_return_400() {
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_400_{suffix}");
    let Some((router, _state, _temp, _schema)) = build_summary_router(schema).await else {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    };

    let mut body = window();
    let to = body["to"].clone();
    body["to"] = body["from"].clone();
    body["from"] = to;
    let (status, v) = search_raw(&router, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "from>to: {v}");

    let mut body = window();
    body["cursor"] = json!("not-a-cursor");
    let (status, v) = search_raw(&router, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "bad cursor: {v}");

    let cursor = {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "t": Utc::now().to_rfc3339(),
                "id": "x"
            }))
            .unwrap(),
        )
    };

    let mut body = window();
    body["order_by"] = json!("error_count");
    body["order"] = json!("desc");
    body["cursor"] = json!(cursor.clone());
    let (status, v) = search_raw(&router, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "cursor+error_count: {v}");

    let mut body = window();
    body["order"] = json!("asc");
    body["cursor"] = json!(cursor);
    let (status, v) = search_raw(&router, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "cursor+asc: {v}");
}

#[tokio::test]
async fn http_session_summary_rereduce_refreshes_counts() {
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_rr_{suffix}");
    let Some((router, state, _temp, schema)) = build_summary_router(schema).await else {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    };

    let spec = SpanSpec {
        session_id: "rr-sess",
        trace: 0xe1,
        start_ago_s: 90,
        duration_s: 40,
        error: false,
        agent: "agent-rr",
        user: "u-rr",
        model: "gpt-4o",
        tokens: 100,
        cost: 0.1,
    };
    // Generation + agent span (agent without usage).
    ingest(&router, llm_span(&spec)).await;
    {
        let mut agent = llm_span(&spec);
        let span = &mut agent.resource_spans[0].scope_spans[0].spans[0];
        span.name = "agent-rr".into();
        span.span_id = vec![0xc1; 8];
        span.attributes.retain(|kv| {
            kv.key == sp::SESSION_ID || kv.key == sp::OBSERVATION_TYPE || kv.key == sp::AGENT_NAME
        });
        for kv in &mut span.attributes {
            if kv.key == sp::OBSERVATION_TYPE {
                *kv = string_kv(sp::OBSERVATION_TYPE, "agent");
            }
        }
        ingest(&router, agent).await;
    }
    flush(&state).await;
    assert!(run_reduce(&state).await >= 1);

    let first = search(&router, window()).await;
    let item = first["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["session_id"] == "rr-sess")
        .expect("rr-sess after first reduce");
    let obs1 = item["observation_count"].as_i64().unwrap();
    let tokens1 = item["total_tokens"].as_i64().unwrap();
    assert!(obs1 >= 2, "gen+agent: {item}");
    assert_eq!(tokens1, 100);

    // Late span: another generation → dirty → re-reduce must refresh.
    let mut late = llm_span(&spec);
    {
        let span = &mut late.resource_spans[0].scope_spans[0].spans[0];
        span.span_id = vec![0xe9; 8];
        span.trace_id = vec![0xe2; 16];
        // Slightly newer end so dirty max_ts moves.
        let end = Utc::now() - ChronoDuration::seconds(20);
        let start = end - ChronoDuration::seconds(5);
        span.start_time_unix_nano = start.timestamp_nanos_opt().unwrap() as u64;
        span.end_time_unix_nano = end.timestamp_nanos_opt().unwrap() as u64;
    }
    ingest(&router, late).await;
    flush(&state).await;
    assert!(
        dirty_count(&state, &schema).await >= 1,
        "late span must re-dirty"
    );
    assert!(run_reduce(&state).await >= 1);

    let second = search(&router, window()).await;
    let item = second["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["session_id"] == "rr-sess")
        .expect("rr-sess after re-reduce");
    let obs2 = item["observation_count"].as_i64().unwrap();
    let tokens2 = item["total_tokens"].as_i64().unwrap();
    assert!(
        obs2 > obs1,
        "re-reduce must raise observation_count ({obs1} → {obs2}): {item}"
    );
    assert_eq!(tokens2, 200, "two generations × 100 tokens: {item}");
}

#[tokio::test]
async fn http_session_summary_list_independent_of_span_volume() {
    // Stage 3.5: list reads session_summary only — latency must not track lake
    // span volume in the window.
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_http_vol_{suffix}");
    let Some((router, state, _temp, _schema)) = build_summary_router(schema).await else {
        eprintln!("skip: ducklake-postgres not reachable");
        return;
    };

    let sparse = SpanSpec {
        session_id: "vol-sparse",
        trace: 0xf1,
        start_ago_s: 120,
        duration_s: 30,
        error: false,
        agent: "agent-vol",
        user: "u-vol",
        model: "gpt-4o",
        tokens: 10,
        cost: 0.01,
    };
    ingest(&router, llm_span(&sparse)).await;

    const FAT_SPANS: usize = 120;
    let fat = SpanSpec {
        session_id: "vol-fat",
        trace: 0xf2,
        start_ago_s: 80,
        duration_s: 20,
        error: false,
        agent: "agent-vol",
        user: "u-vol",
        model: "gpt-4o",
        tokens: 1,
        cost: 0.001,
    };
    for i in 0..FAT_SPANS {
        let mut req = llm_span(&fat);
        let span = &mut req.resource_spans[0].scope_spans[0].spans[0];
        span.span_id = vec![0x10 + (i % 200) as u8; 8];
        span.span_id[0] = (i / 200) as u8;
        span.trace_id = vec![0xf2; 16];
        span.trace_id[0] = (i % 255) as u8;
        span.trace_id[1] = (i / 255) as u8;
        // Stagger within the session so they are distinct.
        let end = Utc::now() - ChronoDuration::seconds(80 - (i as i64 % 40));
        let start = end - ChronoDuration::seconds(1);
        span.start_time_unix_nano = start.timestamp_nanos_opt().unwrap() as u64;
        span.end_time_unix_nano = end.timestamp_nanos_opt().unwrap() as u64;
        ingest(&router, req).await;
    }

    flush(&state).await;
    assert!(run_reduce(&state).await >= 2);

    let body = window();
    let t0 = std::time::Instant::now();
    let list = search(&router, body).await;
    let elapsed = t0.elapsed();

    let ids = session_ids(&list);
    assert!(ids.contains(&"vol-sparse"), "{list}");
    assert!(ids.contains(&"vol-fat"), "{list}");
    let fat_item = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["session_id"] == "vol-fat")
        .unwrap();
    assert!(
        fat_item["observation_count"].as_i64().unwrap() >= FAT_SPANS as i64,
        "fat summary must count lake spans: {fat_item}"
    );
    assert_summary_only_item(fat_item);

    // Absolute bound: if list scanned 120 lake spans it would be far slower /
    // flaky. Summary SELECT must stay snappy.
    assert!(
        elapsed.as_millis() < 750,
        "list must be independent of lake span volume (took {elapsed:?})"
    );

    // Second identical list should also be fast (warm + still summary-only).
    let t1 = std::time::Instant::now();
    let _ = search(&router, window()).await;
    assert!(
        t1.elapsed().as_millis() < 750,
        "repeat list still slow: {:?}",
        t1.elapsed()
    );
}
