//! Periodic OTel export into the ops DuckLake scope + slow-query log drain.

use crate::api::AppState;
use crate::config::Config;
use async_trait::async_trait;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{
    MetricError, MetricResult, PeriodicReader, SdkMeterProvider, Temporality,
};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

use super::convert::metrics_from_resource_metrics;
use super::events::{init_slow_query_channel, to_log};
use super::instruments::{install_instruments, record_export_drop, refresh_process_gauges};
use super::inventory::spawn_inventory_loop;
use super::OPS_TENANT_ID;

static EXPORT_STATE: once_cell::sync::OnceCell<AppState> = once_cell::sync::OnceCell::new();

#[derive(Debug)]
struct DuckLakePushExporter;

#[async_trait]
impl PushMetricExporter for DuckLakePushExporter {
    async fn export(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
        export_inner(metrics).await
    }

    async fn force_flush(&self) -> MetricResult<()> {
        Ok(())
    }

    fn shutdown(&self) -> MetricResult<()> {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

async fn export_inner(metrics: &mut ResourceMetrics) -> MetricResult<()> {
    refresh_process_gauges();
    let Some(state) = EXPORT_STATE.get() else {
        return Ok(());
    };
    let rows = metrics_from_resource_metrics(metrics);
    if rows.is_empty() {
        return Ok(());
    }
    let engine = state
        .engines
        .engine_for(OPS_TENANT_ID)
        .await
        .map_err(|e| MetricError::Other(e.to_string()))?;
    if let Err(err) = engine
        .ingest
        .writer()
        .write_metric_batches(vec![rows])
        .await
    {
        record_export_drop();
        warn!("self-monitoring metric export failed: {err}");
        return Err(MetricError::Other(err.to_string()));
    }
    Ok(())
}

fn spawn_slow_query_drain(state: AppState) {
    let mut rx = init_slow_query_channel();
    tokio::spawn(async move {
        let mut batch = Vec::new();
        let mut flush = tokio::time::interval(Duration::from_secs(2));
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                ev = rx.recv() => {
                    match ev {
                        Some(e) => {
                            batch.push(to_log(&e));
                            if batch.len() >= 32 {
                                flush_logs(&state, &mut batch).await;
                            }
                        }
                        None => {
                            flush_logs(&state, &mut batch).await;
                            break;
                        }
                    }
                }
                _ = flush.tick() => {
                    flush_logs(&state, &mut batch).await;
                }
            }
        }
    });
}

async fn flush_logs(state: &AppState, batch: &mut Vec<crate::models::Log>) {
    if batch.is_empty() {
        return;
    }
    let logs = std::mem::take(batch);
    let res = async {
        let engine = state.engines.engine_for(OPS_TENANT_ID).await?;
        engine.ingest.writer().write_log_batches(vec![logs]).await
    }
    .await;
    if let Err(err) = res {
        record_export_drop();
        warn!("self-monitoring slow-query log export failed: {err}");
    }
}

/// Install SDK PeriodicReader → DuckLake exporter and background scrapers.
pub fn spawn_exporter(state: AppState, config: Arc<Config>) {
    let _ = EXPORT_STATE.set(state.clone());
    let interval = Duration::from_secs(config.self_monitoring.export_interval_seconds.max(1));

    let reader = PeriodicReader::builder(DuckLakePushExporter, Tokio)
        .with_interval(interval)
        .build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "thelake",
        )]))
        .build();
    global::set_meter_provider(provider);
    install_instruments();

    gauge_store_init_from_config(&config);
    spawn_slow_query_drain(state.clone());
    spawn_inventory_loop(state, config.self_monitoring.export_interval_seconds.max(1));
}

fn gauge_store_init_from_config(config: &Config) {
    use std::sync::atomic::Ordering;
    super::gauge_store::QUERY_WORKERS.store(config.query.max_connections.max(1), Ordering::Relaxed);
    super::gauge_store::WRITER_POOL_SIZE.store(config.ducklake.writer_pool_size, Ordering::Relaxed);
}
