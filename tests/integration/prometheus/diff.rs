//! Mini differential: Softprobe Prometheus API vs pinned prom/prometheus:v2.54.1.
//!
//! Requires Docker. Run via `make test-prom-diff`.

use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use softprobe_runtime::compat::prometheus::diff_normalize::normalize_prom_response;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::compat_support::conformance::{
    descriptor_for_case, parse_case_selection, select_cases, select_differential_cases,
    CompatExecutionRecorder,
};

use crate::compat_support::prometheus::{
    encode_query_owned, gauge_series_otlp, get_json, ingest_metrics, sum_series_otlp,
};
use crate::compat_support::prometheus_oracle::{
    build_tenant_router, query_prom_oracle, require_docker, start_prometheus_with_openmetrics,
    EVAL_BASE_SECS,
};

#[derive(Debug, Clone, Deserialize)]
struct PromDiffCase {
    id: String,
    path: String,
    params: BTreeMap<String, String>,
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/compat/prometheus/diff")
}

fn ns(sec: u64) -> u64 {
    sec * 1_000_000_000
}

fn load_diff_cases() -> Vec<PromDiffCase> {
    let path = fixture_dir().join("cases.json");
    serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {path:?}: {error}"))
}

fn write_prometheus_artifacts(
    case: &PromDiffCase,
    lake_raw: Option<&Value>,
    oracle_raw: Option<&Value>,
) -> std::io::Result<PathBuf> {
    crate::compat_support::lifecycle::write_failure_artifacts(
        "prometheus",
        &case.id,
        &case.path,
        &case.params,
        lake_raw,
        oracle_raw,
        normalize_prom_response,
        "PROMETHEUS_RAW_ARTIFACT",
        "PROMETHEUS_NORMALIZED_ARTIFACT",
    )
}

#[tokio::test]
#[ignore = "docker oracle; run via make test-prom-diff"]
async fn mini_diff_vs_pinned_prometheus() {
    let cases = load_diff_cases();
    let selection = parse_case_selection(
        "Prometheus",
        std::env::var("COMPAT_CASE_IDS").ok().as_deref(),
        std::env::var("COMPAT_CASE_ID").ok().as_deref(),
    )
    .unwrap_or_else(|error| panic!("invalid Prometheus differential case selection: {error}"));
    let selected_cases = select_cases(
        "prometheus",
        &cases,
        selection.as_ref(),
        |_| true,
        |case| {
            descriptor_for_case(
                "prometheus",
                &case.id,
                "GET",
                &case.path,
                case.params.clone(),
                true,
            )
        },
    )
    .unwrap_or_else(|error| panic!("invalid Prometheus differential case selection: {error}"));
    assert!(
        !selected_cases.is_empty(),
        "Prometheus differential selection resolved to no cases"
    );
    let descriptors = selected_cases
        .iter()
        .map(|selected| selected.descriptor.clone())
        .collect::<Vec<_>>();
    let mut recorder = CompatExecutionRecorder::new("prometheus", &descriptors, None)
        .expect("create Prometheus execution receipt");
    let expected_case_ids = descriptors
        .iter()
        .map(|descriptor| descriptor.case_id.clone())
        .collect::<BTreeSet<_>>();

    require_docker();

    let dir = fixture_dir();
    let samples = dir.join("samples.openmetrics");
    let cases_path = dir.join("cases.json");
    assert!(samples.is_file(), "missing {samples:?}");
    assert!(cases_path.is_file(), "missing {cases_path:?}");

    let om = std::fs::read_to_string(&samples).expect("read openmetrics");
    let oracle = start_prometheus_with_openmetrics(&om, "thelake-prom-diff");

    // Lake with equivalent samples.
    let (router, _temp) = build_tenant_router().await;
    ingest_metrics(
        &router,
        gauge_series_otlp(
            "http_requests",
            "checkout",
            &[
                (ns(EVAL_BASE_SECS), 10.0),
                (ns(EVAL_BASE_SECS + 60), 20.0),
                (ns(EVAL_BASE_SECS + 120), 40.0),
            ],
        ),
    )
    .await;
    ingest_metrics(
        &router,
        gauge_series_otlp(
            "http_requests",
            "api",
            &[
                (ns(EVAL_BASE_SECS), 5.0),
                (ns(EVAL_BASE_SECS + 60), 15.0),
                (ns(EVAL_BASE_SECS + 120), 35.0),
            ],
        ),
    )
    .await;
    ingest_metrics(
        &router,
        sum_series_otlp(
            "demo_counter",
            "checkout",
            &[
                (ns(EVAL_BASE_SECS), 100.0),
                (ns(EVAL_BASE_SECS + 60), 160.0),
                (ns(EVAL_BASE_SECS + 120), 220.0),
            ],
        ),
    )
    .await;

    let readiness_case = selected_cases.first().expect("selected case");
    let readiness_case_id = readiness_case.case.id.clone();
    let readiness_case_path = readiness_case.case.path.clone();
    let readiness_params = readiness_case
        .case
        .params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let readiness_body = query_prom_oracle(&oracle.base, &readiness_case_path, &readiness_params);
    assert_eq!(
        readiness_body["status"], "success",
        "selected Prometheus readiness {}: {readiness_body}",
        readiness_case_id
    );

    let mut executed = BTreeSet::new();
    let mut readiness_body = Some(readiness_body);
    for selected in selected_cases {
        let case = selected.case;
        let descriptor = &selected.descriptor;
        let id = case.id.as_str();
        let path = case.path.as_str();
        let params = case
            .params
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let oracle_raw = if id == readiness_case_id {
            readiness_body.take().expect("selected readiness response")
        } else {
            query_prom_oracle(&oracle.base, path, &params)
        };

        let qs = encode_query_owned(&params);
        let (status, lake_raw) = get_json(&router, &format!("{path}?{qs}")).await;
        executed.insert(descriptor.case_id.clone());

        if status != StatusCode::OK || lake_raw["status"] != "success" {
            let artifacts = write_prometheus_artifacts(&case, Some(&lake_raw), Some(&oracle_raw))
                .expect("write Prometheus failure artifacts");
            recorder
                .record_case(descriptor, "failure", "softprobe_http_or_envelope_failure")
                .expect("record Prometheus execution");
            recorder
                .finish("failure", "softprobe_http_or_envelope_failure")
                .expect("finish Prometheus execution receipt");
            panic!(
                "Prometheus differential HTTP failure for {id}; artifacts at {}",
                artifacts.display()
            );
        }
        if oracle_raw["status"] != "success" {
            let artifacts = write_prometheus_artifacts(&case, Some(&lake_raw), Some(&oracle_raw))
                .expect("write Prometheus failure artifacts");
            recorder
                .record_case(descriptor, "failure", "oracle_failure")
                .expect("record Prometheus execution");
            recorder
                .finish("failure", "oracle_failure")
                .expect("finish Prometheus execution receipt");
            panic!(
                "Prometheus oracle failure for {id}; artifacts at {}",
                artifacts.display()
            );
        }

        let oracle_n = normalize_prom_response(oracle_raw.clone());
        let lake_n = normalize_prom_response(lake_raw.clone());
        if lake_n["data"]["resultType"] != oracle_n["data"]["resultType"]
            || lake_n["data"]["result"] != oracle_n["data"]["result"]
        {
            let artifacts = write_prometheus_artifacts(&case, Some(&lake_raw), Some(&oracle_raw))
                .expect("write Prometheus failure artifacts");
            recorder
                .record_case(descriptor, "failure", "normalized_mismatch")
                .expect("record Prometheus execution");
            recorder
                .finish("failure", "normalized_mismatch")
                .expect("finish Prometheus execution receipt");
            panic!(
                "Prometheus differential mismatch for {id}; artifacts at {}",
                artifacts.display()
            );
        }

        write_prometheus_artifacts(&case, Some(&lake_raw), Some(&oracle_raw))
            .expect("write Prometheus differential artifacts");
        recorder
            .record_case(descriptor, "pass", "matched")
            .expect("record Prometheus execution");
    }

    recorder
        .finish("pass", "matched")
        .expect("finish Prometheus execution receipt");

    assert_eq!(
        executed, expected_case_ids,
        "Prometheus differential did not execute every selected case"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_selector_filters_prometheus_differential_requests() {
        let cases = load_diff_cases();
        let selection = parse_case_selection(
            "Prometheus",
            Some("prometheus-query-range-selector"),
            Some("__suite__"),
        )
        .expect("valid selector");

        let selected =
            select_differential_cases("Prometheus", &cases, selection.as_ref(), |case| {
                descriptor_for_case(
                    "prometheus",
                    &case.id,
                    "GET",
                    &case.path,
                    case.params.clone(),
                    true,
                )
                .ok()
                .map(|descriptor| descriptor.case_id)
            })
            .expect("selected differential case");

        assert_eq!(
            selected
                .iter()
                .map(|case| case.id.as_str())
                .collect::<Vec<_>>(),
            vec!["range_selector"],
            "manifest selection must transmit only the mapped Prometheus request"
        );
    }

    #[test]
    fn selectors_combine_and_suite_sentinel_is_ignored() {
        let selection = parse_case_selection(
            "Prometheus",
            Some("prometheus-query-selector-instant, prometheus-query-range-selector"),
            Some("__suite__"),
        )
        .expect("valid selector")
        .expect("selected cases");

        assert_eq!(
            selection,
            BTreeSet::from([
                "prometheus-query-range-selector".to_string(),
                "prometheus-query-selector-instant".to_string(),
            ])
        );
    }

    #[test]
    fn selectors_reject_unknown_and_non_differential_manifest_cases() {
        let cases = load_diff_cases();
        for case_id in ["missing-prometheus-case", "prometheus-labels-discovery"] {
            let selection = BTreeSet::from([case_id.to_string()]);
            let error = select_differential_cases("Prometheus", &cases, Some(&selection), |case| {
                descriptor_for_case(
                    "prometheus",
                    &case.id,
                    "GET",
                    &case.path,
                    case.params.clone(),
                    true,
                )
                .ok()
                .map(|descriptor| descriptor.case_id)
            })
            .expect_err("invalid selector");
            assert!(
                error.contains(case_id),
                "error should name {case_id}: {error}"
            );
        }
    }

    #[test]
    fn absent_selector_preserves_all_prometheus_differential_requests() {
        let cases = load_diff_cases();
        let selected = select_differential_cases("Prometheus", &cases, None, |case| {
            descriptor_for_case(
                "prometheus",
                &case.id,
                "GET",
                &case.path,
                case.params.clone(),
                true,
            )
            .ok()
            .map(|descriptor| descriptor.case_id)
        })
        .expect("all differential cases");
        assert_eq!(selected.len(), 3);
        assert_eq!(
            selected
                .iter()
                .map(|case| case.id.as_str())
                .collect::<Vec<_>>(),
            vec!["selector_instant", "sum_by_job", "range_selector"]
        );
    }
}
