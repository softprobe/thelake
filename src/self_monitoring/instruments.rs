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
    /// DuckLake write transactions (flush-through request or coalesce flush).
    pub ingest_commits: Counter<u64>,
    /// Rows included in those commits.
    pub ingest_rows_committed: Counter<u64>,
    /// Coalesce timer / force_flush drains (absent on flush-through).
    pub ingest_coalesce_flushes: Counter<u64>,
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
    /// Prom sample-scan plan: grain table + raw vs downsample vs live UNION.
    pub sample_scans: Counter<u64>,
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
        ingest_commits: meter
            .u64_counter("thelake.ingest.commits")
            .with_description(
                "DuckLake write transactions (one per coalesce flush or flush-through request)",
            )
            .build(),
        ingest_rows_committed: meter
            .u64_counter("thelake.ingest.rows_committed")
            .with_description("Rows written in DuckLake ingest commits")
            .build(),
        ingest_coalesce_flushes: meter
            .u64_counter("thelake.ingest.coalesce_flushes")
            .with_description("Soft-coalesce timer/force_flush drains (not flush-through)")
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
        sample_scans: meter
            .u64_counter("thelake.query.sample_scans")
            .with_description(
                "Prom sample scans by grain table and scan_mode \
                 (raw | downsample | downsample_with_raw_tail)",
            )
            .build(),
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
    // Omit `app` on duration histograms — 64 apps × signals explode SDK series
    // and self-mon export/coalesce drain pegged Softprobe CPU after light ingest.
    let status = if ok { "ok" } else { "error" };
    let a = attrs(&[
        ("tenant", tenant),
        ("signal", signal),
        ("status", status),
        ("op", "ingest"),
    ]);
    if ok {
        let app = bound_app(app);
        i.ingest_requests.add(
            1,
            &attrs(&[
                ("tenant", tenant),
                ("signal", signal),
                ("status", status),
                ("app", &app),
                ("op", "ingest"),
            ]),
        );
    } else {
        i.ingest_errors.add(1, &a);
    }
    i.ingest_duration_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
}

pub fn record_write(tenant: &str, signal: &str, app: Option<&str>, elapsed: Duration) {
    let Some(i) = instruments() else { return };
    let _ = app; // cardinality: duration without per-app series
    let a = attrs(&[
        ("tenant", tenant),
        ("signal", signal),
        ("op", "write"),
        ("status", "ok"),
    ]);
    i.write_duration_ms
        .record(elapsed.as_secs_f64() * 1000.0, &a);
}

/// Record a completed DuckLake ingest commit (coalesced or flush-through).
///
/// When coalesce is on, `rate(commits)` must stay well below `rate(requests)`.
pub fn record_ingest_commit(tenant: &str, signal: &str, rows: u64, coalesced: bool) {
    let Some(i) = instruments() else { return };
    let path = if coalesced {
        "coalesce"
    } else {
        "flush_through"
    };
    let a = attrs(&[
        ("tenant", tenant),
        ("signal", signal),
        ("path", path),
        ("op", "ingest"),
    ]);
    i.ingest_commits.add(1, &a);
    if rows > 0 {
        i.ingest_rows_committed.add(rows, &a);
    }
    if coalesced {
        i.ingest_coalesce_flushes.add(1, &a);
    }
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

/// Count Prom sample-scan plans so ops can see raw vs downsample vs live UNION
/// without inferring from latency alone.
pub fn record_sample_scan(tenant: &str, grain: &str, scan_mode: &str) {
    let Some(i) = instruments() else { return };
    i.sample_scans.add(
        1,
        &attrs(&[
            ("tenant", tenant),
            ("grain", grain),
            ("scan_mode", scan_mode),
            ("op", "query"),
        ]),
    );
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
///
/// Reads `/proc/self` directly — no `sysinfo` double-refresh sleep. That sleep
/// previously ran on the tokio worker (inventory/export) and, with
/// `worker_threads=1`, stalled the whole runtime every scrape.
pub fn refresh_process_gauges() {
    use std::fs;

    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut rss_kb = 0u64;
    let mut vsize_kb = 0u64;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss_kb = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("VmSize:") {
            vsize_kb = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    gauge_store::PROCESS_RSS.store(rss_kb.saturating_mul(1024), Ordering::Relaxed);
    gauge_store::PROCESS_VSIZE.store(vsize_kb.saturating_mul(1024), Ordering::Relaxed);

    let threads = fs::read_dir("/proc/self/task")
        .map(|rd| rd.count() as u64)
        .unwrap_or(0);
    gauge_store::PROCESS_THREADS.store(threads, Ordering::Relaxed);

    // Instantaneous CPU: delta utime+stime vs previous sample (Linux jiffies).
    // Ratio units match prior sysinfo path: 1000 milli ≈ one full core.
    static PREV_CPU: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    static PREV_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        // comm can contain spaces/parens; utime/stime are fields 14/15 after ") ".
        if let Some(rest) = stat.rsplit(") ").next() {
            let fields: Vec<&str> = rest.split_whitespace().collect();
            // After ") ": state is [0], so utime=[11], stime=[12] (1-based 14/15 of full stat).
            if fields.len() > 12 {
                let utime: u64 = fields[11].parse().unwrap_or(0);
                let stime: u64 = fields[12].parse().unwrap_or(0);
                let jiffies = utime.saturating_add(stime);
                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                let prev_j = PREV_CPU.swap(jiffies, Ordering::Relaxed);
                let prev_ns = PREV_NS.swap(now_ns, Ordering::Relaxed);
                if prev_j > 0 && now_ns > prev_ns {
                    let dj = jiffies.saturating_sub(prev_j) as f64;
                    let dt_sec = (now_ns - prev_ns) as f64 / 1e9;
                    // Linux USER_HZ is almost always 100.
                    let cores = (dj / 100.0) / dt_sec.max(1e-6);
                    let cpu_milli = (cores * 1000.0) as u64;
                    gauge_store::PROCESS_CPU_MILLI.store(cpu_milli, Ordering::Relaxed);
                }
            }
        }
    }

    if let Ok(io) = fs::read_to_string("/proc/self/io") {
        for line in io.lines() {
            if let Some(rest) = line.strip_prefix("read_bytes: ") {
                if let Ok(v) = rest.trim().parse::<u64>() {
                    gauge_store::PROCESS_DISK_READ.store(v, Ordering::Relaxed);
                }
            } else if let Some(rest) = line.strip_prefix("write_bytes: ") {
                if let Ok(v) = rest.trim().parse::<u64>() {
                    gauge_store::PROCESS_DISK_WRITE.store(v, Ordering::Relaxed);
                }
            }
        }
    }
}
