//! Periodic OTel metrics export via standard OTLP (not into DuckLake).

use crate::api::AppState;
use crate::config::Config;
use async_trait::async_trait;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::MetricExporter;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{MetricResult, PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use super::instruments::{install_instruments, refresh_process_gauges};
use super::inventory::spawn_inventory_loop;

/// Wraps the OTLP exporter so process gauges refresh on each export tick.
#[derive(Debug)]
struct RefreshingOtlpExporter {
    inner: MetricExporter,
}

#[async_trait]
impl PushMetricExporter for RefreshingOtlpExporter {
    async fn export(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
        refresh_process_gauges();
        self.inner.export(metrics).await
    }

    async fn force_flush(&self) -> MetricResult<()> {
        self.inner.force_flush().await
    }

    fn shutdown(&self) -> MetricResult<()> {
        self.inner.shutdown()
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

/// OTel resource for metrics: honor `OTEL_SERVICE_NAME` / `OTEL_SERVICE_INSTANCE_ID` /
/// `HOSTNAME`, plus any `OTEL_RESOURCE_ATTRIBUTES` (via `Resource::default()`).
///
/// Always sets semantic `service.name`, `service.instance.id`, and `host.name` so
/// Prometheus series can be filtered per process (local bench vs production).
fn metrics_resource() -> Resource {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".into());
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "thelake".to_string());
    let instance_id = std::env::var("OTEL_SERVICE_INSTANCE_ID").unwrap_or_else(|_| {
        resource_attr_from_env("service.instance.id")
            .unwrap_or_else(|| format!("{host}-{}", std::process::id()))
    });
    let host = resource_attr_from_env("host.name").unwrap_or(host);

    Resource::default().merge(&Resource::new(vec![
        KeyValue::new("service.name", service_name),
        KeyValue::new("service.instance.id", instance_id),
        KeyValue::new("host.name", host),
    ]))
}

fn resource_attr_from_env(key: &str) -> Option<String> {
    let raw = std::env::var("OTEL_RESOURCE_ATTRIBUTES").ok()?;
    for part in raw.split(',') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            if k.trim() == key {
                let v = v.trim();
                if !v.is_empty() {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}

/// Install SDK PeriodicReader → OTLP metrics exporter and background scrapers.
///
/// Destination uses standard `OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*`
/// environment variables. Fails soft if the exporter cannot be built.
pub fn spawn_exporter(state: AppState, config: Arc<Config>) {
    let interval = Duration::from_secs(config.self_monitoring.export_interval_seconds.max(1));

    let exporter = match MetricExporter::builder().with_http().build() {
        Ok(e) => e,
        Err(err) => {
            warn!("self-monitoring OTLP metrics exporter build failed (continuing without export): {err}");
            return;
        }
    };

    let reader = PeriodicReader::builder(RefreshingOtlpExporter { inner: exporter }, Tokio)
        .with_interval(interval)
        .build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(metrics_resource())
        .build();
    global::set_meter_provider(provider);
    install_instruments();

    gauge_store_init_from_config(&config);
    spawn_inventory_loop(state, config.self_monitoring.export_interval_seconds.max(1));
    info!(
        interval_secs = config.self_monitoring.export_interval_seconds.max(1),
        "self-monitoring OTLP metrics export started"
    );
}

fn gauge_store_init_from_config(config: &Config) {
    use std::sync::atomic::Ordering;
    super::gauge_store::QUERY_WORKERS.store(config.query.max_connections.max(1), Ordering::Relaxed);
    super::gauge_store::WRITER_POOL_SIZE.store(config.ducklake.writer_pool_size, Ordering::Relaxed);
}

/// Unit-test helper: ensure OTLP exporter builder succeeds with default env.
#[cfg(test)]
pub fn try_build_otlp_exporter() -> Result<(), String> {
    MetricExporter::builder()
        .with_http()
        .build()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::metrics_resource;

    #[test]
    fn metrics_resource_sets_service_and_instance() {
        // Clear can be racy in parallel tests; only assert keys we always set when
        // OTEL_RESOURCE_ATTRIBUTES does not already define them.
        let r = metrics_resource();
        let keys: Vec<String> = r.iter().map(|(k, _)| k.to_string()).collect();
        assert!(
            keys.iter().any(|k| k == "service.name"),
            "missing service.name in {keys:?}"
        );
        assert!(
            keys.iter().any(|k| k == "service.instance.id" || k == "host.name"),
            "expected instance or host in {keys:?}"
        );
    }
}
