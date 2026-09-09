//! Cross-window PromQL correctness: backdated ingest across Grafana windows.
//!
//! Guards the regression where mid-range boards (6h/12h/24h) went empty because
//! the planner selected empty `metric_samples_5m` / `metric_samples_1h` and only
//! returned a few minutes of raw lag-tail — including when `rate(...[5m])`
//! lookback tipped an exact-24h client window past the raw boundary.
//!
//! Every supported live window must return a non-empty series whose timestamps
//! cover most of the requested span, for both identity and `rate()` queries.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::compat::backends::grain::{
    select_sample_grain, SampleGrain, FIVE_MIN_RANGE_MS, RAW_RANGE_MS,
};
use softprobe_runtime::config::Config;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::compat_support::prometheus::{
    encode_query_pairs, get_json, ingest_metrics, sum_series_otlp,
};
use crate::util::config::file_backed_test_config;
use crate::util::tenant::inject_local_sqlite_tenant;

const METRIC: &str = "window_correctness_counter";
const JOB: &str = "window-correctness";

/// Grafana-style windows exercised end-to-end (name, span_s, step_s).
const WINDOWS_S: &[(&str, i64, i64)] = &[
    ("5m", 5 * 60, 15),
    ("15m", 15 * 60, 15),
    ("30m", 30 * 60, 15),
    ("1h", 60 * 60, 15),
    ("3h", 3 * 60 * 60, 30),
    ("6h", 6 * 60 * 60, 30),
    ("12h", 12 * 60 * 60, 60),
    ("24h", 24 * 60 * 60, 60),
    ("2d", 2 * 24 * 60 * 60, 120),
    ("7d", 7 * 24 * 60 * 60, 300),
    ("30d", 30 * 24 * 60 * 60, 3600),
];

/// How far back we seed samples (covers 30d boards with slack).
const SEED_SPAN_S: i64 = 32 * 24 * 3600;

async fn build_router(config: Config) -> Router {
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_local_sqlite_tenant))
}

async fn post_sql(router: &Router, sql: &str) -> StatusCode {
    let body = serde_json::json!({ "sql": sql });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    resp.status()
}

/// Backdated counter with adaptive cadence: 1m for the last 48h (dense boards),
/// 5m older than that. Value = seconds since seed start so `rate()` ≈ 1.0.
fn backdated_counter_samples(end_s: i64, span_s: i64) -> Vec<(u64, f64)> {
    let start_s = end_s - span_s;
    let dense_from = end_s - 48 * 3600;
    let mut out = Vec::new();
    let mut t = start_s;
    loop {
        let value = (t - start_s) as f64;
        out.push(((t as u64).saturating_mul(1_000_000_000), value));
        if t >= end_s {
            break;
        }
        let step = if t >= dense_from { 60 } else { 300 };
        let next = t + step;
        t = if next >= end_s { end_s } else { next };
    }
    out
}

async fn materialize_5m_and_1h(router: &Router) {
    let steps = [
        r#"
INSERT INTO softprobe.metric_samples_5m
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '5 minutes', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '5 minutes', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM softprobe.metric_samples
WHERE timestamp < now() - INTERVAL '5 minutes'
  AND time_bucket(INTERVAL '5 minutes', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM softprobe.metric_samples_5m)
GROUP BY series_id, time_bucket(INTERVAL '5 minutes', timestamp)
"#,
        r#"
INSERT INTO softprobe.metric_samples_1h
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '1 hour', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '1 hour', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM softprobe.metric_samples
WHERE timestamp < now() - INTERVAL '1 hour'
  AND time_bucket(INTERVAL '1 hour', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM softprobe.metric_samples_1h)
GROUP BY series_id, time_bucket(INTERVAL '1 hour', timestamp)
"#,
    ];
    for sql in steps {
        let status = post_sql(router, sql.trim()).await;
        assert!(
            status.is_success(),
            "materialize step failed: http={status}"
        );
    }
}

fn result_points(body: &serde_json::Value) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    let rows = body
        .pointer("/data/result")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for series in rows {
        for pair in series
            .get("values")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
        {
            let ts = pair.get(0).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let val = pair
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            out.push((ts, val));
        }
    }
    out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    out
}

#[test]
fn planner_mid_windows_use_raw_not_empty_5m() {
    let end = chrono::Utc::now().timestamp_millis();
    for &(name, secs, step) in WINDOWS_S {
        if secs > 24 * 3600 {
            continue;
        }
        let start = end - secs * 1000;
        let g = select_sample_grain(Some(start), Some(end), Some(step * 1000), false);
        assert_eq!(
            g,
            SampleGrain::Raw,
            "{name}: mid-window must use raw (got {g:?}); empty 5m would blank boards"
        );
        assert!(secs * 1000 <= RAW_RANGE_MS);
    }
    let g2d = select_sample_grain(
        Some(end - 2 * 24 * 3600 * 1000),
        Some(end),
        Some(120_000),
        false,
    );
    assert_eq!(g2d, SampleGrain::FiveMin);
    assert!(2 * 24 * 3600 * 1000 <= FIVE_MIN_RANGE_MS);

    let g7d = select_sample_grain(
        Some(end - 7 * 24 * 3600 * 1000),
        Some(end),
        Some(300_000),
        false,
    );
    assert_eq!(g7d, SampleGrain::FiveMin);

    let g30d = select_sample_grain(
        Some(end - 30 * 24 * 3600 * 1000),
        Some(end),
        Some(3_600_000),
        false,
    );
    assert_eq!(g30d, SampleGrain::OneHour);
}

#[tokio::test]
async fn query_range_covers_all_grafana_windows_after_backdated_ingest() {
    let temp = TempDir::new().unwrap();
    let config = file_backed_test_config(&temp);
    let router = build_router(config).await;

    // Seed 32d of counter samples ending ~now (rate ≈ 1.0).
    let end_s = chrono::Utc::now().timestamp() - 30;
    let samples = backdated_counter_samples(end_s, SEED_SPAN_S);
    assert!(
        samples.len() > 10_000,
        "expected dense+sparse seed, got {}",
        samples.len()
    );
    for chunk in samples.chunks(500) {
        ingest_metrics(&router, sum_series_otlp(METRIC, JOB, chunk)).await;
    }
    materialize_5m_and_1h(&router).await;

    let end_q = chrono::Utc::now().timestamp();
    let data_cap = (SEED_SPAN_S as f64) * 0.70;

    for &(name, secs, step) in WINDOWS_S {
        let start = end_q - secs;
        let path = format!(
            "/api/v1/query_range?{}",
            encode_query_pairs(&[
                ("query", METRIC),
                ("start", &start.to_string()),
                ("end", &end_q.to_string()),
                ("step", &step.to_string()),
            ])
        );
        let (status, body) = get_json(&router, &path).await;
        assert_eq!(status, StatusCode::OK, "{name}: http status");
        assert_eq!(
            body.get("status").and_then(|v| v.as_str()),
            Some("success"),
            "{name}: prom status {:?}",
            body.get("error")
        );
        let pts = result_points(&body);
        assert!(
            !pts.is_empty(),
            "{name}: expected non-empty series (empty 5m/raw-tail regression)"
        );
        let span = pts.last().unwrap().0 - pts.first().unwrap().0;
        let expect_span = (secs as f64) * 0.70;
        let need = expect_span.min(data_cap);
        assert!(
            span >= need,
            "{name}: timestamp span {span:.0}s < {need:.0}s (pts={}, first={}, last={})",
            pts.len(),
            pts.first().unwrap().0,
            pts.last().unwrap().0
        );
        let mut prev = f64::NEG_INFINITY;
        for (_, v) in &pts {
            assert!(
                *v + 1e-9 >= prev,
                "{name}: counter went backwards ({prev} → {v})"
            );
            prev = *v;
        }
    }
}

/// Cart-style `sum(rate(metric[5m]))` must cover every Grafana window — including
/// exact 24h where lookback expands fetch past the raw grain boundary.
#[tokio::test]
async fn rate_query_covers_all_grafana_windows_after_backdated_ingest() {
    let temp = TempDir::new().unwrap();
    let config = file_backed_test_config(&temp);
    let router = build_router(config).await;

    let end_s = chrono::Utc::now().timestamp() - 30;
    let samples = backdated_counter_samples(end_s, SEED_SPAN_S);
    for chunk in samples.chunks(500) {
        ingest_metrics(&router, sum_series_otlp(METRIC, JOB, chunk)).await;
    }
    materialize_5m_and_1h(&router).await;

    let end_q = chrono::Utc::now().timestamp();
    let expr = format!("sum(rate({METRIC}[5m]))");
    // Multi-day boards keep ≤24h raw for rate[5m]; older 5m/1h `last` cannot
    // feed a 5m range selector (one point per bucket).
    let raw_cover_s = 24.0 * 3600.0;

    for &(name, secs, step) in WINDOWS_S {
        let start = end_q - secs;
        let path = format!(
            "/api/v1/query_range?{}",
            encode_query_pairs(&[
                ("query", expr.as_str()),
                ("start", &start.to_string()),
                ("end", &end_q.to_string()),
                ("step", &step.to_string()),
            ])
        );
        let (status, body) = get_json(&router, &path).await;
        assert_eq!(status, StatusCode::OK, "{name}: http status");
        assert_eq!(
            body.get("status").and_then(|v| v.as_str()),
            Some("success"),
            "{name}: prom status {:?}",
            body.get("error")
        );
        let pts = result_points(&body);
        assert!(
            !pts.is_empty(),
            "{name}: rate() empty (lookback→empty-5m regression?)"
        );
        let span = pts.last().unwrap().0 - pts.first().unwrap().0;
        let window_need = (secs as f64) * 0.65;
        let need = if secs <= 24 * 3600 {
            window_need
        } else {
            window_need.min(raw_cover_s * 0.65)
        };
        assert!(
            span >= need,
            "{name}: rate span {span:.0}s < {need:.0}s (pts={})",
            pts.len()
        );
        // Seeded seconds-since-start → rate ≈ 1.0; allow downsample/stitch slack.
        let mid = pts[pts.len() / 2].1;
        assert!(
            mid > 0.5 && mid < 1.5,
            "{name}: expected rate≈1.0 at mid-window, got {mid} (pts={})",
            pts.len()
        );
    }
}
