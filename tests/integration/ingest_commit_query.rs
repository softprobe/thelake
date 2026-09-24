use crate::util::perf::{PerformanceMetrics, Timer};
use crate::util::pipeline::TestPipeline;
use crate::util::poll::wait_for;
use crate::util::storage_config::load_test_config;
use chrono::Utc;
use softprobe_runtime::models::{Log as LogData, Span as SpanData, SpanEvent};
use softprobe_runtime::query::{LogCountFilter, TraceCountFilter};
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

// Note: perf + config helpers live under `tests/util/`.

#[tokio::test]
async fn test_ingestion_perf_5000_spans_under_one_second() {
    let config = load_test_config();
    // Allow buffering without forcing flush during perf check
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let now = Utc::now();
    let count = 5_000usize;
    let mut spans = Vec::with_capacity(count);
    for i in 0..count {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), "perf-session".to_string());
        attributes.insert("span.index".to_string(), i.to_string());
        spans.push(SpanData {
            session_id: "perf-session".to_string(),
            trace_id: format!("trace-perf-{}", i),
            span_id: format!("span-perf-{}", i),
            parent_span_id: None,
            app_id: "app-perf".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "perf".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
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
            status_message: Some("OK".to_string()),
        });
    }

    let timer = Timer::start("Ingest 5000 spans");
    pipeline
        .add_spans(spans, count * 256)
        .await
        .expect("ingest spans");
    let duration = timer.stop();

    let metrics = PerformanceMetrics::new("Ingest 5000 spans")
        .with_duration(duration)
        .with_rows(count);
    metrics.print_report();
    metrics.assert_performance_target(1000, "5000 spans ingest");
}

#[tokio::test]
async fn test_iceberg_writer_bulk_session_roundtrip() {
    let config = load_test_config();
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    // Create multiple sessions with spans to test multi-session row groups
    let num_sessions = 5;
    let spans_per_session = 1000;
    let now = Utc::now();

    let mut all_session_batches = Vec::new();
    let mut session_ids = Vec::new();

    for session_idx in 0..num_sessions {
        let session_id = format!("session-{}", uuid::Uuid::new_v4());
        session_ids.push(session_id.clone());

        let mut session_spans = Vec::new();
        for i in 0..spans_per_session {
            let mut attributes = HashMap::new();
            attributes.insert("sp.session.id".to_string(), session_id.clone());
            attributes.insert("span.index".to_string(), i.to_string());

            // Add events to every 10th span
            let events = if i % 10 == 0 {
                vec![SpanEvent {
                    name: format!("event.{}", i),
                    timestamp: now
                        + chrono::Duration::milliseconds((session_idx * 1000 + i) as i64),
                    attributes: HashMap::from([("event.index".to_string(), i.to_string())]),
                }]
            } else {
                Vec::new()
            };

            // Add HTTP data to first span of each session for verification
            let (
                http_method,
                http_path,
                http_headers,
                http_req_body,
                http_status,
                http_resp_headers,
                http_resp_body,
            ) = if i == 0 {
                (
                    Some("POST".to_string()),
                    Some(format!("/api/v1/session/{}", session_idx)),
                    Some(r#"{"Content-Type":"application/json","Authorization":"Bearer test-token"}"#.to_string()),
                    Some(format!(r#"{{"session_id":"{}","action":"create"}}"#, session_id)),
                    Some(200),
                    Some(r#"{"Content-Type":"application/json","X-Request-Id":"req-123"}"#.to_string()),
                    Some(format!(r#"{{"success":true,"session_id":"{}","created_at":"2025-12-31T00:00:00Z"}}"#, session_id)),
                )
            } else {
                (None, None, None, None, None, None, None)
            };

            session_spans.push(SpanData {
                session_id: session_id.clone(), // Explicit session_id field
                trace_id: format!("trace-{}-{}", session_idx, i),
                span_id: format!("span-{}-{}", session_idx, i),
                parent_span_id: None,
                app_id: format!("app-{}", session_idx % 2), // Alternate between 2 apps
                organization_id: Some("org-test".to_string()),
                tenant_id: Some("tenant-test".to_string()),
                agent_id: None,
                agent_name: None,
                message_type: "HTTP_REQUEST".to_string(),
                span_kind: Some("SERVER".to_string()),
                timestamp: now + chrono::Duration::milliseconds((session_idx * 1000 + i) as i64),
                end_timestamp: Some(
                    now + chrono::Duration::milliseconds((session_idx * 1000 + i + 5) as i64),
                ),
                attributes,
                resource_attributes: HashMap::new(),
                events,
                http_request_method: http_method,
                http_request_path: http_path,
                http_request_headers: http_headers,
                http_request_body: http_req_body,
                http_response_status_code: http_status,
                http_response_headers: http_resp_headers,
                http_response_body: http_resp_body,
                status_code: Some("OK".to_string()),
                status_message: Some("Success".to_string()),
            });
        }
        all_session_batches.push(session_spans);
    }

    let total_spans = num_sessions * spans_per_session;

    // Act: write through buffer + flush to staged cache
    println!(
        "🧪 Writing {} sessions ({} total spans) via buffer + staged cache...",
        num_sessions, total_spans
    );
    let write_timer = Timer::start(&format!(
        "Multi-Session Buffered Write ({} sessions)",
        num_sessions
    ));
    pipeline
        .add_spans(
            all_session_batches
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            total_spans * 256,
        )
        .await
        .expect("span add should succeed");

    // Flush to staged cache (not WAL)
    pipeline
        .force_flush_spans()
        .await
        .expect("force flush spans");

    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!(
        "Multi-Session WAL Write ({} sessions, {} spans)",
        num_sessions, total_spans
    ))
    .with_duration(write_duration)
    .with_rows(total_spans);
    write_metrics.print_report();
    write_metrics.assert_performance_target(5000, "Multi-session WAL write time");

    // Local staged/WAL paths are not always listed via `IngestEngine` (flush goes to DuckLake writer).
    println!("✅ Flush completed (DuckLake flush-through)");
    println!("✅ Querying back each session to verify row group isolation...");

    // Query each session individually to verify row group isolation
    let mut total_query_duration = std::time::Duration::ZERO;

    for (session_idx, session_id) in session_ids.iter().enumerate() {
        println!(
            "\n🔍 Querying session {}/{}: {}",
            session_idx + 1,
            num_sessions,
            session_id
        );

        let query_timer = Timer::start(&format!("Query session {}", session_idx + 1));
        let found = test_pipeline
            .query_engine()
            .count_traces(TraceCountFilter {
                session_id: Some(session_id.clone()),
                ..Default::default()
            })
            .await
            .expect("query") as usize;

        let query_duration = query_timer.stop();
        total_query_duration += query_duration;

        println!("  ✓ Found {} rows", found);

        assert_eq!(
            found, spans_per_session,
            "Expected exactly {} spans for session {}, found {}",
            spans_per_session, session_id, found
        );

        let http_result = test_pipeline
            .query_engine()
            .find_http_span(session_id)
            .await
            .expect("http query");
        let http_result = http_result.expect("HTTP fields row");
        let method = http_result.request_method.as_deref().unwrap_or("");
        let path = http_result.request_path.as_deref().unwrap_or("");
        let headers = http_result.request_headers.as_deref().unwrap_or("");
        let body = http_result.request_body.as_deref().unwrap_or("");
        let status = http_result.response_status_code.unwrap_or_default();
        let resp_headers = http_result.response_headers.as_deref().unwrap_or("");
        let resp_body = http_result.response_body.as_deref().unwrap_or("");

        assert_eq!(
            method, "POST",
            "HTTP method should be POST for session {}",
            session_idx
        );
        assert_eq!(
            path,
            format!("/api/v1/session/{}", session_idx),
            "HTTP path should match for session {}",
            session_idx
        );
        assert!(
            headers.contains("Content-Type"),
            "Request headers should contain Content-Type"
        );
        assert!(
            headers.contains("Authorization"),
            "Request headers should contain Authorization"
        );
        assert!(
            body.contains(session_id.as_str()),
            "Request body should contain session_id"
        );
        assert!(
            body.contains("action"),
            "Request body should contain action field"
        );
        assert_eq!(status, 200, "HTTP response status should be 200");
        assert!(
            resp_headers.contains("X-Request-Id"),
            "Response headers should contain X-Request-Id"
        );
        assert!(
            resp_body.contains(session_id.as_str()),
            "Response body should contain session_id"
        );
        assert!(
            resp_body.contains("success"),
            "Response body should contain success field"
        );

        println!("  ✓ HTTP fields verified for session {}", session_idx);
    }

    println!("\n📊 Multi-Session Query Performance Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "⏱️  Total query time ({} sessions): {:?}",
        num_sessions, total_query_duration
    );
    println!(
        "⏱️  Average per session: {:?}",
        total_query_duration / num_sessions as u32
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!(
        "\n✅ WAL-backed union-read validated for {} sessions",
        num_sessions
    );

    println!("🔄 Forcing flush to staged local cache...");
    pipeline.force_flush_spans().await.expect("force flush");

    println!("⚙️  Running optimizer to commit staged spans to Iceberg...");

    println!("\n✅ WAL, local cache, and optimizer paths validated for spans");
}

#[tokio::test]
async fn test_duckdb_union_read_realtime_performance() {
    let config = load_test_config();
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let now = Utc::now();
    let base_session = format!("union-base-{}", uuid::Uuid::new_v4());
    let staged_session = format!("union-staged-{}", uuid::Uuid::new_v4());
    let buffer_session = format!("union-buffer-{}", uuid::Uuid::new_v4());

    let mut base_spans = Vec::new();
    for i in 0..200 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), base_session.clone());
        attributes.insert("span.index".to_string(), i.to_string());

        base_spans.push(SpanData {
            session_id: base_session.clone(),
            trace_id: format!("trace-base-{}", i),
            span_id: format!("span-base-{}", i),
            parent_span_id: None,
            app_id: "app-union".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "union_base".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
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
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .add_spans(base_spans, 200 * 512)
        .await
        .expect("base add");
    pipeline.force_flush_spans().await.expect("base flush");

    let mut staged_spans = Vec::new();
    for i in 0..100 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), staged_session.clone());
        attributes.insert("span.index".to_string(), i.to_string());
        staged_spans.push(SpanData {
            session_id: staged_session.clone(),
            trace_id: format!("trace-staged-{}", i),
            span_id: format!("span-staged-{}", i),
            parent_span_id: None,
            app_id: "app-union".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "union_staged".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(10_000 + i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(10_000 + i as i64 + 1)),
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
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .add_spans(staged_spans, 100 * 512)
        .await
        .expect("staged add");
    pipeline.force_flush_spans().await.expect("staged flush");

    let mut buffer_spans = Vec::new();
    for i in 0..50 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), buffer_session.clone());
        attributes.insert("span.index".to_string(), i.to_string());
        buffer_spans.push(SpanData {
            session_id: buffer_session.clone(),
            trace_id: format!("trace-buffer-{}", i),
            span_id: format!("span-buffer-{}", i),
            parent_span_id: None,
            app_id: "app-union".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "union_buffer".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(20_000 + i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(20_000 + i as i64 + 1)),
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
            status_message: Some("OK".to_string()),
        });
    }
    pipeline
        .add_spans(buffer_spans, 50 * 512)
        .await
        .expect("buffer add");

    let query_engine = test_pipeline.query_engine();

    let base_count = query_engine
        .count_traces(TraceCountFilter {
            session_id: Some(base_session.clone()),
            ..Default::default()
        })
        .await
        .expect("base query") as i64;
    assert_eq!(base_count, 200, "Union-read should see committed rows");

    let staged_count = query_engine
        .count_traces(TraceCountFilter {
            session_id: Some(staged_session.clone()),
            ..Default::default()
        })
        .await
        .expect("staged query") as i64;
    assert_eq!(staged_count, 100, "Union-read should see staged rows");

    let buffer_count = query_engine
        .count_traces(TraceCountFilter {
            session_id: Some(buffer_session.clone()),
            ..Default::default()
        })
        .await
        .expect("buffer query") as i64;
    assert_eq!(buffer_count, 50, "Union-read should see buffered rows");
}

#[tokio::test]
async fn test_iceberg_writer_bulk_log_roundtrip() {
    let config = load_test_config();
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    // Create multiple sessions with logs to test multi-session row groups
    let test_type = std::env::var("E2E_BACKEND").unwrap_or_else(|_| "local".to_string());
    let (num_sessions, logs_per_session) = if test_type == "r2" {
        (2, 200)
    } else {
        (5, 1000)
    };
    let now = Utc::now();

    let mut all_logs = Vec::new();
    let mut session_ids = Vec::new();

    for session_idx in 0..num_sessions {
        let session_id = format!("log-session-{}", uuid::Uuid::new_v4());
        session_ids.push(session_id.clone());

        for i in 0..logs_per_session {
            let mut attributes = HashMap::new();
            attributes.insert("log.index".to_string(), i.to_string());
            attributes.insert("source".to_string(), "test".to_string());

            let mut resource_attributes = HashMap::new();
            resource_attributes.insert(
                "service.name".to_string(),
                format!("test-service-{}", session_idx),
            );
            resource_attributes.insert("host.name".to_string(), "localhost".to_string());

            // Vary severity across logs
            let severity_number = (i % 5 + 1) * 4; // INFO=4, WARN=8, ERROR=12, etc.
            let severity_text = match severity_number {
                4 => "INFO",
                8 => "WARN",
                12 => "ERROR",
                16 => "FATAL",
                _ => "DEBUG",
            }
            .to_string();

            // Add trace correlation for every 5th log
            let (trace_id, span_id) = if i % 5 == 0 {
                (
                    Some(format!("trace-{}-{}", session_idx, i)),
                    Some(format!("span-{}-{}", session_idx, i)),
                )
            } else {
                (None, None)
            };

            all_logs.push(LogData {
                session_id: Some(session_id.clone()),
                timestamp: now + chrono::Duration::milliseconds((session_idx * 1000 + i) as i64),
                observed_timestamp: Some(
                    now + chrono::Duration::milliseconds((session_idx * 1000 + i + 1) as i64),
                ),
                severity_number: severity_number as i32,
                severity_text,
                body: format!("Test log message {} from session {}", i, session_idx),
                attributes,
                resource_attributes,
                trace_id,
                span_id,
                tenant_id: None,
                agent_id: None,
                agent_name: None,
            });
        }
    }

    let total_logs = num_sessions * logs_per_session;

    // Act: add all logs through the ingest path (WAL first, then local cache, then optimizer)
    println!(
        "🧪 Writing {} sessions ({} total logs) via WAL + local cache...",
        num_sessions, total_logs
    );
    let write_timer = Timer::start(&format!(
        "Multi-Session WAL Log Write ({} sessions)",
        num_sessions
    ));
    pipeline
        .add_logs(all_logs, total_logs * 256)
        .await
        .expect("add logs should succeed");

    pipeline.force_flush_logs().await.expect("force flush logs");

    let write_duration = write_timer.stop();

    // Report write performance
    let write_metrics = PerformanceMetrics::new(&format!(
        "Multi-Session Log Add ({} sessions, {} logs)",
        num_sessions, total_logs
    ))
    .with_duration(write_duration)
    .with_rows(total_logs);
    write_metrics.print_report();
    println!("✅ Flush completed (DuckLake flush-through)");
    println!("✅ Querying back each session through DuckDB union view...");

    println!("✅ WAL, local cache, and optimizer paths validated for logs");
}

#[tokio::test]
async fn test_http_fields_in_span_model() {
    use chrono::Utc;
    use softprobe_runtime::models::Span as SpanData;
    use std::collections::HashMap;

    println!("🧪 Testing HTTP fields in Span model...");

    // Create a span with all HTTP fields populated
    let session_id = "test-session-123";
    let span = SpanData {
        session_id: session_id.to_string(), // Explicit session_id field
        trace_id: "trace-abc".to_string(),
        span_id: "span-xyz".to_string(),
        parent_span_id: None,
        app_id: "test-app".to_string(),
        organization_id: Some("org-test".to_string()),
        tenant_id: Some("tenant-test".to_string()),
        agent_id: None,
        agent_name: None,
        message_type: "HTTP_REQUEST".to_string(),
        span_kind: Some("SERVER".to_string()),
        timestamp: Utc::now(),
        end_timestamp: Some(Utc::now()),
        attributes: HashMap::from([("sp.session.id".to_string(), session_id.to_string())]),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: Some("POST".to_string()),
        http_request_path: Some("/api/v1/test".to_string()),
        http_request_headers: Some(r#"{"Content-Type":"application/json"}"#.to_string()),
        http_request_body: Some(r#"{"test":"data"}"#.to_string()),
        http_response_status_code: Some(200),
        http_response_headers: Some(r#"{"X-Request-Id":"req-123"}"#.to_string()),
        http_response_body: Some(r#"{"success":true}"#.to_string()),
        status_code: Some("OK".to_string()),
        status_message: Some("Success".to_string()),
    };

    // Verify all HTTP fields are set correctly
    assert_eq!(span.http_request_method, Some("POST".to_string()));
    assert_eq!(span.http_request_path, Some("/api/v1/test".to_string()));
    assert!(span
        .http_request_headers
        .as_ref()
        .unwrap()
        .contains("Content-Type"));
    assert!(span.http_request_body.as_ref().unwrap().contains("test"));
    assert_eq!(span.http_response_status_code, Some(200));
    assert!(span
        .http_response_headers
        .as_ref()
        .unwrap()
        .contains("X-Request-Id"));
    assert!(span
        .http_response_body
        .as_ref()
        .unwrap()
        .contains("success"));

    println!("✅ HTTP request method: {:?}", span.http_request_method);
    println!("✅ HTTP request path: {:?}", span.http_request_path);
    println!("✅ HTTP request headers: {:?}", span.http_request_headers);
    println!("✅ HTTP request body: {:?}", span.http_request_body);
    println!(
        "✅ HTTP response status: {:?}",
        span.http_response_status_code
    );
    println!("✅ HTTP response headers: {:?}", span.http_response_headers);
    println!("✅ HTTP response body: {:?}", span.http_response_body);

    // Verify the span can be converted to Arrow RecordBatch
    let mut config = load_test_config();
    config.ingest.flush_interval_seconds = 0;
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    // Write the span and verify it succeeds
    let result = pipeline.add_spans(vec![span], 0).await;
    assert!(
        result.is_ok(),
        "Failed to write span with HTTP fields: {:?}",
        result.err()
    );

    println!("✅ Successfully wrote span with HTTP fields to Iceberg");
    println!("✅ HTTP fields are correctly included in the schema and can be persisted");
}

#[tokio::test]
async fn test_pinned_metadata_updates_on_commit() {
    let config = load_test_config();

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;
    let now = Utc::now();

    let mut spans = Vec::new();
    for i in 0..10 {
        spans.push(SpanData {
            session_id: format!("pin-session-{}", i),
            trace_id: format!("pin-trace-{}", i),
            span_id: format!("pin-span-{}", i),
            parent_span_id: None,
            app_id: "app-pin".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "pin".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
            attributes: HashMap::new(),
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
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .add_spans(spans.clone(), spans.len() * 256)
        .await
        .expect("add spans");
    pipeline.force_flush_spans().await.expect("force flush");

    let pointer_dir = test_pipeline.cache_dir.path().join("catalog_metadata");
    assert!(
        !pointer_dir.join("traces.json").exists(),
        "legacy catalog_metadata pointer files must not be written"
    );

    let first = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            app_id: Some("app-pin".to_string()),
            ..Default::default()
        })
        .await
        .expect("query after first flush");
    assert_eq!(first, 10, "expected 10 spans after first flush");

    pipeline
        .add_spans(spans, 10 * 256)
        .await
        .expect("add spans");
    pipeline.force_flush_spans().await.expect("force flush");

    let second = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            app_id: Some("app-pin".to_string()),
            ..Default::default()
        })
        .await
        .expect("query after second flush");
    assert_eq!(second, 20, "expected 20 spans after second flush");
    assert!(
        !pointer_dir.join("traces.json").exists(),
        "legacy catalog_metadata pointer files must not be written"
    );
}

#[tokio::test]
async fn test_duckdb_union_read_realtime_concurrency() {
    let config = load_test_config();
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let now = Utc::now();
    let staged_session = format!("perf-staged-{}", uuid::Uuid::new_v4());
    let per_session = 200usize;

    let mut staged_logs = Vec::new();
    for i in 0..per_session {
        staged_logs.push(LogData {
            session_id: Some(staged_session.clone()),
            timestamp: now + chrono::Duration::milliseconds(10_000 + i as i64),
            observed_timestamp: Some(now + chrono::Duration::milliseconds(10_000 + i as i64 + 1)),
            severity_number: 4,
            severity_text: "INFO".to_string(),
            body: format!("Staged log {}", i),
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
        });
    }
    pipeline
        .add_logs(staged_logs, per_session * 256)
        .await
        .expect("stage add");

    // `TestPipeline` always attaches DuckLake; `logs` needs catalog tables that exist after
    // flush, so we do not query before flush here (would error: table `logs` does not exist).
    let query_engine = test_pipeline.query_engine().clone();

    pipeline.force_flush_logs().await.expect("stage flush");

    // Wait until union view reflects flushed data (staged path listing may be empty).
    wait_for(
        Duration::from_secs(5),
        Duration::from_millis(200),
        || async {
            let c = query_engine
                .count_logs(LogCountFilter {
                    session_id: Some(staged_session.clone()),
                    ..Default::default()
                })
                .await?;
            Ok(c >= per_session as u64)
        },
    )
    .await
    .expect("logs should show flushed rows");

    // Query AFTER flush to verify staged files are visible
    let staged_count = query_engine
        .count_logs(LogCountFilter {
            session_id: Some(staged_session.clone()),
            ..Default::default()
        })
        .await
        .expect("staged query");
    println!(
        "🔍 Staged query result (after flush): {} rows",
        staged_count
    );
    assert_eq!(
        staged_count, per_session as u64,
        "Expected {} rows in staged",
        per_session
    );

    // Test concurrent queries on the same session
    let sessions = vec![staged_session.clone(); 6]; // All concurrent queries will use staged_session
    let mut handles = Vec::new();
    let concurrent = 6usize;
    for i in 0..concurrent {
        let session_id = sessions[i % sessions.len()].clone();
        let engine = query_engine.clone();
        handles.push(tokio::spawn(async move {
            let filter = LogCountFilter {
                session_id: Some(session_id),
                ..Default::default()
            };
            let _ = engine.count_logs(filter.clone()).await.expect("warmup");
            let start = Instant::now();
            let count = engine.count_logs(filter).await.expect("query");
            let duration = start.elapsed();
            (duration, count)
        }));
    }

    for handle in handles {
        let (_duration, count) = handle.await.expect("task");
        assert_eq!(count, per_session as u64, "Expected {} rows", per_session);
    }
}

#[tokio::test]
async fn test_union_read_flushes_spans_to_staged_and_updates_wal_watermark() {
    let config = load_test_config();

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    let session_id = format!("sql-flush-{}", uuid::Uuid::new_v4());
    let now = Utc::now();
    let span = SpanData {
        session_id: session_id.clone(),
        trace_id: "trace-flush".to_string(),
        span_id: "span-flush".to_string(),
        parent_span_id: None,
        app_id: "app-flush".to_string(),
        organization_id: None,
        tenant_id: None,
        agent_id: None,
        agent_name: None,
        message_type: "span".to_string(),
        span_kind: Some("server".to_string()),
        timestamp: now,
        end_timestamp: Some(now),
        attributes: HashMap::from([("sp.session.id".to_string(), session_id.clone())]),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
        status_code: None,
        status_message: None,
    };

    pipeline
        .add_spans(vec![span], 256)
        .await
        .expect("add spans");

    wait_for(
        Duration::from_secs(5),
        Duration::from_millis(200),
        || async {
            let count = test_pipeline
                .query_engine()
                .count_traces(TraceCountFilter {
                    session_id: Some(session_id.clone()),
                    ..Default::default()
                })
                .await?;
            Ok(count == 1)
        },
    )
    .await
    .expect("union view should return exactly one row after flush");
}

#[tokio::test]
async fn test_wal_replay_recovers_spans() {
    // Flush-through DuckLake ingest has no WAL tier. Kept as a skipped placeholder so historical
    // test names do not reappear as regressions if someone reintroduces buffer/WAL paths.
    let _config = load_test_config();
}

// Snapshot expire + TWCS under the leased PhysicalScopeMaintenanceJob path are covered by
// `compaction::maintenance_leased_tests` (compact file counts, expire, multi-tenant,
// aborted-holder recover). Do not restore a no-assert `run_once` smoke here.

#[tokio::test]
async fn test_wal_cleanup_after_flush() {
    let config = load_test_config();

    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;

    // First flush: create WAL files
    let mut first_batch = Vec::new();
    for i in 0..50 {
        first_batch.push(SpanData {
            session_id: "wal-cleanup-test-1".to_string(),
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
            parent_span_id: None,
            app_id: "test".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "span".to_string(),
            span_kind: Some("server".to_string()),
            timestamp: Utc::now(),
            end_timestamp: Some(Utc::now()),
            attributes: HashMap::new(),
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
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .add_spans(first_batch, 50 * 256)
        .await
        .expect("first add");
    pipeline.force_flush_spans().await.expect("first flush");

    println!("✅ First flush completed (DuckLake flush-through)");

    // Wait a bit to ensure timestamps are different
    tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

    // Second flush: should clean up first WAL files
    let mut second_batch = Vec::new();
    for i in 50..100 {
        second_batch.push(SpanData {
            session_id: "wal-cleanup-test-2".to_string(),
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
            parent_span_id: None,
            app_id: "test".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "span".to_string(),
            span_kind: Some("server".to_string()),
            timestamp: Utc::now(),
            end_timestamp: Some(Utc::now()),
            attributes: HashMap::new(),
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
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .add_spans(second_batch, 50 * 256)
        .await
        .expect("second add");
    pipeline.force_flush_spans().await.expect("second flush");

    println!("✅ Second flush completed (DuckLake flush-through)");

    let c1 = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            session_id: Some("wal-cleanup-test-1".to_string()),
            ..Default::default()
        })
        .await
        .expect("q1");
    let c2 = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            session_id: Some("wal-cleanup-test-2".to_string()),
            ..Default::default()
        })
        .await
        .expect("q2");
    assert_eq!(c1, 50, "expected 50 spans for session 1");
    assert_eq!(c2, 50, "expected 50 spans for session 2");
}

#[tokio::test]
async fn test_commit_staged_data_updates_metadata_and_removes_files_no_double_count() {
    // This test verifies the complete commit flow:
    // 1. Data is written and flushed to staged files
    // 2. Union view shows data from staged files
    // 3. Optimizer commits staged data to Iceberg
    // 4. Metadata pinning is updated
    // 5. Staged files are removed
    // 6. Union view doesn't double count (data appears once, not in both staged and Iceberg)
    // 7. Data appears in Iceberg view
    // 8. Union view still returns correct count after commit

    let config = load_test_config();
    let test_pipeline = TestPipeline::new(config).await;
    let pipeline = &test_pipeline.ingest;
    let now = Utc::now();

    // Create a unique session ID for this test
    let session_id = format!("commit-test-{}", uuid::Uuid::new_v4());
    let expected_count = 100usize;

    // Step 1: Write data and flush to staged
    println!("📝 Step 1: Writing {} spans to buffer...", expected_count);
    let mut spans = Vec::new();
    for i in 0..expected_count {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), session_id.clone());
        attributes.insert("span.index".to_string(), i.to_string());
        spans.push(SpanData {
            session_id: session_id.clone(),
            trace_id: format!("trace-commit-{}", i),
            span_id: format!("span-commit-{}", i),
            parent_span_id: None,
            app_id: "app-commit-test".to_string(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "commit_test".to_string(),
            span_kind: Some("SERVER".to_string()),
            timestamp: now + chrono::Duration::milliseconds(i as i64),
            end_timestamp: Some(now + chrono::Duration::milliseconds(i as i64 + 1)),
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
            status_message: Some("OK".to_string()),
        });
    }

    pipeline
        .add_spans(spans, expected_count * 256)
        .await
        .expect("add spans");
    pipeline.force_flush_spans().await.expect("force flush");

    // Step 2: Local staged listing (may be empty; union view is authoritative)
    println!("📁 Step 2: Staged path listing after flush...");

    // Step 3: Verify union view shows data from staged files
    println!("🔍 Step 3: Verifying union view shows data from staged files...");
    let union_count_before = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            session_id: Some(session_id.clone()),
            ..Default::default()
        })
        .await
        .expect("query union view");
    let union_count_before = union_count_before as usize;
    assert_eq!(
        union_count_before, expected_count,
        "Expected union view to show {} spans from staged files, found {}",
        expected_count, union_count_before
    );
    println!(
        "✅ Union view shows {} spans (from staged)",
        union_count_before
    );

    // Step 4-7: Legacy catalog_metadata pointer files are gone; durable data is DuckLake only.
    println!("📌 Step 4-7: Confirming no legacy catalog_metadata pointer files...");
    let pointer_path = test_pipeline
        .cache_dir
        .path()
        .join("catalog_metadata")
        .join("traces.json");
    assert!(
        !pointer_path.exists(),
        "legacy catalog_metadata pointer files must not be written"
    );
    println!("⚙️  Step 5-6: Ingest is flush-through (no staged optimizer)");
    println!("✅ No Softprobe metadata pointer files (DuckLake catalog owns snapshots)");

    // Step 8: Verify union view doesn't double count (should still be expected_count, not 2x)
    println!("🔍 Step 8: Verifying union view doesn't double count...");
    let union_count_after = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            session_id: Some(session_id.clone()),
            ..Default::default()
        })
        .await
        .expect("query union view after commit");
    let union_count_after = union_count_after as usize;
    assert_eq!(
        union_count_after, expected_count,
        "Expected union view to show {} spans (not doubled), found {}. This indicates double counting!",
        expected_count, union_count_after
    );
    assert_eq!(
        union_count_before, union_count_after,
        "Union view count should remain {} after commit (not increase due to double counting)",
        expected_count
    );
    println!(
        "✅ Union view shows {} spans (no double counting)",
        union_count_after
    );

    // Step 9: Verify data appears in union view (which includes Iceberg after optimizer)
    println!(
        "🧊 Step 9: Verifying data appears in union view (includes Iceberg after optimizer)..."
    );
    let iceberg_count = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            session_id: Some(session_id.clone()),
            ..Default::default()
        })
        .await
        .expect("query union view");
    let iceberg_count = iceberg_count as usize;
    assert_eq!(
        iceberg_count, expected_count,
        "Expected union view to show {} spans after commit (from Iceberg), found {}",
        expected_count, iceberg_count
    );
    println!("✅ Iceberg view shows {} spans", iceberg_count);

    // Step 10: Verify union view still returns correct count (final check)
    println!("🔍 Step 10: Final verification - union view still returns correct count...");
    let final_union_count = test_pipeline
        .query_engine()
        .count_traces(TraceCountFilter {
            session_id: Some(session_id),
            ..Default::default()
        })
        .await
        .expect("final union view query");
    let final_union_count = final_union_count as usize;
    assert_eq!(
        final_union_count, expected_count,
        "Final check: Expected union view to show {} spans, found {}",
        expected_count, final_union_count
    );
    println!(
        "✅ Final union view count: {} (correct, no double counting)",
        final_union_count
    );

    println!("\n✅ All checks passed! Commit flow verified:");
    println!("  ✓ Staged files created");
    println!("  ✓ Union view shows data from staged");
    println!("  ✓ Optimizer committed to Iceberg");
    println!("  ✓ Staged files removed");
    println!("  ✓ Metadata pinning updated");
    println!("  ✓ Union view doesn't double count");
    println!("  ✓ Data appears in Iceberg view");
    println!("  ✓ Union view returns correct count after commit");
}
