//! Backend-neutral DuckLake Parquet ZSTD compression contract.
//!
//! SQLite and PostgreSQL adapters share ingest + compaction scenarios so catalog-
//! specific ATTACH / `set_option` failures cannot reach production undetected.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use prost::Message;
use softprobe_runtime::compaction::executor::{CompactionStatus, MaintenanceExecutor};
use softprobe_runtime::config::Config;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower::ServiceExt;

use crate::util::otlp::llm_generation_request;

#[async_trait]
pub trait ZstdCompressionBackend: Sync {
    fn router(&self) -> Router;
    fn config(&self) -> Arc<Config>;
    fn data_path(&self) -> &Path;
    fn bearer_token(&self) -> Option<&str>;
    async fn flush_spans(&self);
}

pub fn collect_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out
}

pub fn parquet_count(dir: &Path) -> usize {
    collect_parquet_files(dir).len()
}

pub fn assert_parquet_files_are_zstd(files: &[PathBuf]) {
    assert!(
        !files.is_empty(),
        "expected at least one DuckLake Parquet data file"
    );
    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    for parquet_path in files {
        let escaped = parquet_path.to_string_lossy().replace('\'', "''");
        let codecs: Vec<String> = conn
            .prepare(&format!(
                "SELECT DISTINCT compression FROM parquet_metadata('{escaped}')"
            ))
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(
            codecs,
            vec!["ZSTD".to_string()],
            "DuckLake data file compression for {}: {:?}",
            parquet_path.display(),
            codecs
        );
    }
}

async fn ingest_generation(
    backend: &impl ZstdCompressionBackend,
    session_id: &str,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) {
    let mut buf = Vec::new();
    llm_generation_request(session_id, trace_id, span_id)
        .encode(&mut buf)
        .expect("encode");
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf");
    if let Some(token) = backend.bearer_token() {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let req = builder.body(Body::from(buf)).unwrap();
    let resp = backend.router().oneshot(req).await.expect("ingest");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "ingest body={}",
        String::from_utf8_lossy(&body)
    );
    backend.flush_spans().await;
}

/// Durable DuckLake Parquet under `data_path` must use ZSTD (not Snappy).
pub async fn contract_ingest_parquet_zstd(backend: &impl ZstdCompressionBackend) {
    ingest_generation(backend, "sess-zstd", [0x52; 16], [0x62; 8]).await;
    assert_parquet_files_are_zstd(&collect_parquet_files(backend.data_path()));
}

/// Compaction/merge must emit new ZSTD Parquet (codec set before merge).
pub async fn contract_compaction_keeps_parquet_zstd(backend: &impl ZstdCompressionBackend) {
    for (i, (trace, span)) in [([0x53u8; 16], [0x63u8; 8]), ([0x54u8; 16], [0x64u8; 8])]
        .into_iter()
        .enumerate()
    {
        ingest_generation(backend, &format!("sess-zstd-compact-{i}"), trace, span).await;
    }

    let before = collect_parquet_files(backend.data_path());
    assert!(
        before.len() >= 2,
        "need ≥2 Parquet files before merge; found {}",
        before.len()
    );
    assert_parquet_files_are_zstd(&before);
    let before_names: HashSet<_> = before
        .iter()
        .map(|p| p.file_name().unwrap().to_owned())
        .collect();

    let config = backend.config();
    let maintenance = MaintenanceExecutor::new(config.as_ref(), None, None)
        .await
        .expect("maintenance");
    let summary = maintenance.run_once().await.expect("maintenance run");
    let traces = summary
        .tables
        .iter()
        .find(|t| t.table.ends_with("traces"))
        .unwrap_or_else(|| panic!("missing traces maintenance row: {summary:?}"));
    assert_eq!(
        traces.compaction.status,
        CompactionStatus::Completed,
        "expected merge to complete so post-merge codec is exercised: {summary:?}"
    );

    let after = collect_parquet_files(backend.data_path());
    let new_files: Vec<_> = after
        .iter()
        .filter(|p| !before_names.contains(p.file_name().unwrap()))
        .cloned()
        .collect();
    assert!(
        !new_files.is_empty(),
        "merge Completed but produced no new Parquet file (before={before:?}, after={after:?})"
    );
    assert_parquet_files_are_zstd(&new_files);
}
