//! OTel Meter instruments: Counters, Histograms, ObservableGauges.

use once_cell::sync::OnceCell;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::gauge_store;
use super::labels::{attrs, bound_app};

static EXPORT_DROPS: AtomicU64 = AtomicU64::new(0);

static INSTRUMENTS: OnceCell<Arc<Instruments>> = OnceCell::new();

pub struct Instruments {
    pub ingest_requests: Counter<u64>,
    pub ingest_errors: Counter<u64>,
    pub ingest_duration_ms: Histogram<f64>,
    pub write_duration_ms: Histogram<f64>,
    pub query_duration_ms: Histogram<f64>,
    pub query_queue_wait_ms: Histogram<f64>,
    pub maintenance_passes: Counter<u64>,
    pub compaction_passes: Counter<u64>,
    pub compaction_waves: Counter<u64>,
    pub compaction_duration_ms: Histogram<f64>,
    pub orphan_remove: Counter<u64>,
    pub snapshot_expire: Counter<u64>,
    pub slow_queries: Counter<u64>,
    pub export_drops: Counter<u64>,
}

fn register_observables(meter: &Meter) {
    let _ = meter
        .u64_observable_gauge("thelake.table.live_files")
        .with_description("Live DuckLake data files per tenant table")
        .with_callback(|observer| {
            for entry in gauge_store::TABLE_INV.iter() {
                let k = entry.key();
                observer.observe(
                    entry.value().live_files,
                    &attrs(&[("tenant", &k.tenant), ("table", &k.table)]),
                );
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.table.live_bytes")
        .with_callback(|observer| {
            for entry in gauge_store::TABLE_INV.iter() {
                let k = entry.key();
                observer.observe(
                    entry.value().live_bytes,
                    &attrs(&[("tenant", &k.tenant), ("table", &k.table)]),
                );
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.table.open_day_live_files")
        .with_callback(|observer| {
            for entry in gauge_store::TABLE_INV.iter() {
                let k = entry.key();
                observer.observe(
                    entry.value().open_day_live_files,
                    &attrs(&[("tenant", &k.tenant), ("table", &k.table)]),
                );
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.table.files_by_size_bucket")
        .with_callback(|observer| {
            for entry in gauge_store::SIZE_BUCKETS.iter() {
                let (tenant, table, bucket) = entry.key();
                observer.observe(
                    *entry.value(),
                    &attrs(&[
                        ("tenant", tenant),
                        ("table", table),
                        ("size_bucket", bucket),
                    ]),
                );
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.compaction.files_before")
        .with_callback(|observer| {
            for entry in gauge_store::COMPACTION_FILES_BEFORE.iter() {
                let (tenant, table, day_kind) = entry.key();
                observer.observe(
                    *entry.value(),
                    &attrs(&[("tenant", tenant), ("table", table), ("day_kind", day_kind)]),
                );
            }
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.compaction.files_after")
        .with_callback(|observer| {
            for entry in gauge_store::COMPACTION_FILES_AFTER.iter() {
                let (tenant, table, day_kind) = entry.key();
                observer.observe(
                    *entry.value(),
                    &attrs(&[("tenant", tenant), ("table", table), ("day_kind", day_kind)]),
                );
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge("thelake.process.resident_memory_bytes")
        .with_callback(|observer| {
            observer.observe(gauge_store::PROCESS_RSS.load(Ordering::Relaxed), &[]);
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.process.virtual_memory_bytes")
        .with_callback(|observer| {
            observer.observe(gauge_store::PROCESS_VSIZE.load(Ordering::Relaxed), &[]);
        })
        .build();
    let _ = meter
        .f64_observable_gauge("thelake.process.cpu_ratio")
        .with_callback(|observer| {
            let milli = gauge_store::PROCESS_CPU_MILLI.load(Ordering::Relaxed);
            observer.observe(milli as f64 / 1000.0, &[]);
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.process.thread_count")
        .with_callback(|observer| {
            observer.observe(gauge_store::PROCESS_THREADS.load(Ordering::Relaxed), &[]);
        })
        .build();
    let _ = meter
        .u64_observable_counter("thelake.process.disk_read_bytes")
        .with_callback(|observer| {
            observer.observe(gauge_store::PROCESS_DISK_READ.load(Ordering::Relaxed), &[]);
        })
        .build();
    let _ = meter
        .u64_observable_counter("thelake.process.disk_written_bytes")
        .with_callback(|observer| {
            observer.observe(gauge_store::PROCESS_DISK_WRITE.load(Ordering::Relaxed), &[]);
        })
        .build();

    let _ = meter
        .u64_observable_gauge("thelake.query.workers")
        .with_callback(|observer| {
            observer.observe(
                gauge_store::QUERY_WORKERS.load(Ordering::Relaxed) as u64,
                &[],
            );
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.query.workers_busy")
        .with_callback(|observer| {
            observer.observe(
                gauge_store::QUERY_WORKERS_BUSY.load(Ordering::Relaxed) as u64,
                &[],
            );
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.ingest.pending_batches")
        .with_callback(|observer| {
            observer.observe(
                gauge_store::INGEST_PENDING_BATCHES.load(Ordering::Relaxed) as u64,
                &[],
            );
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.writer.pool_size")
        .with_callback(|observer| {
            observer.observe(
                gauge_store::WRITER_POOL_SIZE.load(Ordering::Relaxed) as u64,
                &[],
            );
        })
        .build();

    let _ = meter
        .u64_observable_counter("thelake.self_heal.rebuilds")
        .with_callback(|observer| {
            let snap = crate::query::duckdb::self_heal_snapshot();
            observer.observe(snap.rebuilds, &[]);
        })
        .build();
    let _ = meter
        .u64_observable_gauge("thelake.self_heal.consecutive_failures")
        .with_callback(|observer| {
            let snap = crate::query::duckdb::self_heal_snapshot();
            observer.observe(snap.consecutive_failures, &[]);
        })
        .build();
}

fn build_instruments(meter: &Meter) -> Instruments {
    Instruments {
        ingest_requests: meter
            .u64_counter("thelake.ingest.requests")
            .with_description("Successful customer OTLP ingest requests")
            .build(),
        ingest_errors: meter
            .u64_counter("thelake.ingest.errors")
            .with_description("Failed customer OTLP ingest requests")
            .build(),
        ingest_duration_ms: meter
            .f64_histogram("thelake.ingest.duration")
            .with_description("Ingest request duration")
            .with_unit("ms")
            .build(),
        write_duration_ms: meter
            .f64_histogram("thelake.write.duration")
            .with_unit("ms")
            .build(),
        query_duration_ms: meter
            .f64_histogram("thelake.query.duration")
            .with_unit("ms")
            .build(),
        query_queue_wait_ms: meter
            .f64_histogram("thelake.query.queue_wait")
            .with_unit("ms")
            .build(),
        maintenance_passes: meter.u64_counter("thelake.maintenance.passes").build(),
        compaction_passes: meter.u64_counter("thelake.compaction.passes").build(),
        compaction_waves: meter.u64_counter("thelake.compaction.waves").build(),
        compaction_duration_ms: meter
            .f64_histogram("thelake.compaction.duration")
            .with_unit("ms")
            .build(),
        orphan_remove: meter.u64_counter("thelake.orphan.remove").build(),
        snapshot_expire: meter.u64_counter("thelake.snapshot.expire").build(),
        slow_queries: meter.u64_counter("thelake.slow_queries").build(),
        export_drops: meter
            .u64_counter("thelake.self_monitoring.export_drops")
            .build(),
    }
}

/// Install global meter provider (call once from bootstrap with PeriodicReader already attached).
pub fn install_instruments() -> Arc<Instruments> {
    let meter = global::meter("thelake");
    register_observables(&meter);
    let inst = Arc::new(build_instruments(&meter));
    // Publish zero so ops dashboards always resolve the series (rate/or panels
    // stay valid before the first real drop).
    inst.export_drops.add(0, &[]);
    let _ = INSTRUMENTS.set(inst.clone());
    inst
}

fn instruments() -> Option<&'static Arc<Instruments>> {
    INSTRUMENTS.get()
}

/// Ensure instruments exist for unit tests that record without full bootstrap.
#[cfg(test)]
pub fn ensure_noop_instruments_for_test() {
    if INSTRUMENTS.get().is_some() {
        return;
    }
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
    global::set_meter_provider(provider);
    install_instruments();
}

pub fn self_monitoring_export_drops() -> u64 {
    EXPORT_DROPS.load(Ordering::Relaxed)
}

pub fn record_export_drop() {
    EXPORT_DROPS.fetch_add(1, Ordering::Relaxed);
    if let Some(i) = instruments() {
        i.export_drops.add(1, &[]);
    }
}

pub fn record_ingest(tenant: &str, signal: &str, ok: bool, app: Option<&str>, elapsed: Duration) {
    let Some(i) = instruments() else { return };
    let app = bound_app(app);
    let status = if ok { "ok" } else { "error" };
    let a = attrs(&[
        ("tenant", tenant),
        ("signal", signal),
        ("status", status),
        ("app", &app),
        ("op", "ingest"),
    ]);
    if ok {
        i.ingest_requests.add(1, &a);
    } else {
        i.ingest_errors.add(1, &a);
    }
    i.ingest_duration_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
}

pub fn record_write(tenant: &str, signal: &str, app: Option<&str>, elapsed: Duration) {
    let Some(i) = instruments() else { return };
    let app = bound_app(app);
    let a = attrs(&[
        ("tenant", tenant),
        ("signal", signal),
        ("app", &app),
        ("op", "write"),
        ("status", "ok"),
    ]);
    i.write_duration_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
}

pub fn record_query(tenant: &str, sql_kind: &str, elapsed: Duration) {
    let Some(i) = instruments() else { return };
    let a = attrs(&[
        ("tenant", tenant),
        ("sql_kind", sql_kind),
        ("op", "query"),
        ("status", "ok"),
    ]);
    i.query_duration_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
}

pub fn record_query_queue_wait(tenant: &str, sql_kind: &str, elapsed: Duration) {
    let Some(i) = instruments() else { return };
    let a = attrs(&[("tenant", tenant), ("sql_kind", sql_kind), ("op", "query")]);
    i.query_queue_wait_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
}

pub fn record_maintenance() {
    let Some(i) = instruments() else { return };
    i.maintenance_passes
        .add(1, &attrs(&[("op", "maintenance"), ("status", "ok")]));
}

pub fn record_compaction_pass(tenant: &str, ok: bool) {
    let Some(i) = instruments() else { return };
    let status = if ok { "ok" } else { "error" };
    i.compaction_passes.add(
        1,
        &attrs(&[("tenant", tenant), ("status", status), ("op", "compact")]),
    );
}

pub fn record_compaction_wave(
    tenant: &str,
    table: &str,
    day_kind: &str,
    elapsed: Duration,
    files_before: u64,
    files_after: u64,
) {
    let Some(i) = instruments() else { return };
    let a = attrs(&[
        ("tenant", tenant),
        ("table", table),
        ("day_kind", day_kind),
        ("op", "compact"),
    ]);
    i.compaction_waves.add(1, &a);
    i.compaction_duration_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
    gauge_store::set_compaction_files(tenant, table, day_kind, files_before, files_after);
}

pub fn record_orphan_remove(tenant: &str, status: &str) {
    let Some(i) = instruments() else { return };
    i.orphan_remove
        .add(1, &attrs(&[("tenant", tenant), ("status", status)]));
}

pub fn record_snapshot_expire(tenant: &str, status: &str) {
    let Some(i) = instruments() else { return };
    i.snapshot_expire
        .add(1, &attrs(&[("tenant", tenant), ("status", status)]));
}

pub fn record_slow_query(tenant: &str, sql_kind: &str) {
    let Some(i) = instruments() else { return };
    i.slow_queries.add(
        1,
        &attrs(&[("tenant", tenant), ("sql_kind", sql_kind), ("op", "query")]),
    );
}

/// Refresh process CPU/RSS/IO snapshots for ObservableGauges (best-effort).
pub fn refresh_process_gauges() {
    use sysinfo::{Pid, ProcessesToUpdate, System};
    let mut sys = System::new();
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    if let Some(p) = sys.process(pid) {
        gauge_store::PROCESS_RSS.store(p.memory(), Ordering::Relaxed);
        gauge_store::PROCESS_VSIZE.store(p.virtual_memory(), Ordering::Relaxed);
        // cpu_usage is percent of one core (100.0 = one full core). Store ×10 so
        // ObservableGauge can expose ratio ≈ stored/1000 (0–N cores).
        let cpu_milli = (p.cpu_usage() as f64 * 10.0) as u64;
        gauge_store::PROCESS_CPU_MILLI.store(cpu_milli, Ordering::Relaxed);
        let threads = p.tasks().map(|t| t.len() as u64).unwrap_or(0);
        gauge_store::PROCESS_THREADS.store(threads, Ordering::Relaxed);
        let disk = p.disk_usage();
        gauge_store::PROCESS_DISK_READ.store(disk.total_read_bytes, Ordering::Relaxed);
        gauge_store::PROCESS_DISK_WRITE.store(disk.total_written_bytes, Ordering::Relaxed);
    }
}
