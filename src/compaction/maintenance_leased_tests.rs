//! Leased [`MaintenanceJob`] integration: real TWCS compact + snapshot expire
//! through `spawn_runner` (not executor `run_once` alone).
//!
//! Covers: single scope compact+expire, multi-tenant scopes, aborted holder
//! recover via lease steal. Intervals use short configurable Duration (50ms+).

use crate::async_jobs::{spawn_runner, Job, LeaseStore, MemoryLeaseStore};
use crate::compaction::executor::MaintenanceExecutor;
use crate::compaction::maintenance_job::MaintenanceJob;
use crate::compaction::scheduler::start_maintenance_scheduler;
use crate::config::{Config, DuckLakeConfig};
use crate::storage::schema::metrics_layout::ensure_metrics_layout_family_tables;
use chrono::{Duration as ChronoDuration, Utc};
use duckdb::Connection;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn maint_test_config(temp: &TempDir) -> Config {
    let mut config = crate::test_support::file_backed_test_config(temp);
    // Force Parquet-per-batch so TWCS has files to merge.
    config.ducklake.data_inlining_row_limit = Some(0);
    config.maintenance.enabled = true;
    config.maintenance.metadata_enabled = true;
    // Short enough for tests; production default is 60s.
    config.maintenance.interval_seconds = 1;
    config.maintenance.max_snapshot_age_seconds = 1;
    config.maintenance.remove_orphan_older_than_seconds = 1;
    config.async_jobs.lease_ttl_seconds = 5;
    config.async_jobs.heartbeat_seconds = 1;
    config.async_jobs.instance_id = Some("maint-test".into());
    config
}

fn ducklake_under(temp: &TempDir, name: &str) -> DuckLakeConfig {
    let duck_dir = temp.path().join(name);
    std::fs::create_dir_all(duck_dir.join("data")).expect("ducklake data");
    DuckLakeConfig {
        catalog_type: "sqlite".into(),
        metadata_path: duck_dir
            .join("metadata.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: duck_dir.join("data").to_string_lossy().into_owned() + "/",
        data_inlining_row_limit: Some(0),
        ..DuckLakeConfig::default()
    }
}

fn open_scope(ducklake: &DuckLakeConfig) -> (Connection, String) {
    crate::storage::ducklake::open_and_attach_ducklake(ducklake).expect("attach")
}

fn live_sample_files(conn: &Connection, catalog: &str) -> i64 {
    conn.query_row(
        &format!(
            "SELECT count(*) FROM __ducklake_metadata_{catalog}.ducklake_data_file df \
             JOIN __ducklake_metadata_{catalog}.ducklake_table t ON df.table_id = t.table_id \
             WHERE t.table_name = 'metric_samples' AND df.end_snapshot IS NULL \
               AND t.end_snapshot IS NULL"
        ),
        [],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

fn snapshot_count(conn: &Connection, catalog: &str) -> i64 {
    conn.query_row(
        &format!("SELECT count(*) FROM __ducklake_metadata_{catalog}.ducklake_snapshot"),
        [],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

fn sample_row_count(conn: &Connection, catalog: &str) -> i64 {
    conn.query_row(
        &format!("SELECT count(*) FROM {catalog}.metric_samples"),
        [],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

/// Closed-day inserts → many small Parquet files + snapshot storm for expire.
fn seed_closed_day_small_files(ducklake: &DuckLakeConfig, series_base: u64) -> (i64, i64, i64) {
    let (conn, catalog) = open_scope(ducklake);
    ensure_metrics_layout_family_tables(&conn, &catalog).expect("layout");

    let day = (Utc::now() - ChronoDuration::days(3))
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    for i in 0..8u64 {
        let sid = series_base + i;
        conn.execute_batch(&format!(
            "INSERT INTO {catalog}.metric_series \
               (series_id, metric_name, metric_type, unit, description, aggregation_temporality, is_monotonic, labels, timestamp) VALUES \
               ({sid}, 'leased_maint', 'gauge', '', '', NULL, NULL, map([], []), TIMESTAMPTZ '{day} 00:00:00+00');\n\
             INSERT INTO {catalog}.metric_samples VALUES \
               ({sid}, TIMESTAMPTZ '{day} 12:0{i}:00+00', {i}.0);"
        ))
        .unwrap_or_else(|e| panic!("seed series_base={series_base} i={i}: {e}"));
    }
    // Extra commits → more snapshots for expire to chew on.
    for i in 0..20u64 {
        conn.execute_batch(&format!(
            "INSERT INTO {catalog}.metric_samples VALUES \
               ({}, TIMESTAMPTZ '{day} 13:00:{i:02}+00', {i}.0);",
            series_base
        ))
        .unwrap_or_else(|e| panic!("extra snap seed i={i}: {e}"));
    }

    let files = live_sample_files(&conn, &catalog);
    let snaps = snapshot_count(&conn, &catalog);
    let rows = sample_row_count(&conn, &catalog);
    assert!(
        files >= 8,
        "precondition: need many live Parquet files, got {files}"
    );
    assert!(
        snaps >= 20,
        "precondition: need snapshot storm, got {snaps}"
    );
    drop(conn);
    (files, snaps, rows)
}

async fn wait_until<F>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    check()
}

fn try_observe(ducklake: &DuckLakeConfig) -> Result<(i64, i64, i64), String> {
    let (conn, catalog) =
        crate::storage::ducklake::open_and_attach_ducklake(ducklake).map_err(|e| e.to_string())?;
    let out = (
        live_sample_files(&conn, &catalog),
        snapshot_count(&conn, &catalog),
        sample_row_count(&conn, &catalog),
    );
    drop(conn);
    Ok(out)
}

fn observe(ducklake: &DuckLakeConfig) -> (i64, i64, i64) {
    // Retry briefly — writer may briefly hold sqlite lock mid-pass.
    for _ in 0..20 {
        if let Ok(v) = try_observe(ducklake) {
            return v;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    try_observe(ducklake).expect("observe ducklake after retries")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leased_maintenance_compacts_files_and_expires_snapshots() {
    let temp = TempDir::new().unwrap();
    let config = maint_test_config(&temp);
    let (files_before, snaps_before, rows_before) =
        seed_closed_day_small_files(&config.ducklake, 100);
    // Age snapshots past max_snapshot_age_seconds=1.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let executor = MaintenanceExecutor::new(&config, None)
        .await
        .expect("executor");
    let job: Arc<dyn Job> = Arc::new(MaintenanceJob::new(
        executor,
        Duration::from_millis(100),
        true,
    ));
    let leases = Arc::new(MemoryLeaseStore::new());
    let handle =
        spawn_runner(&config.async_jobs, leases as Arc<dyn LeaseStore>, vec![job]).expect("runner");

    let ducklake = config.ducklake.clone();
    let ok = wait_until(Duration::from_secs(30), || {
        let (files, snaps, rows) = observe(&ducklake);
        files < files_before && snaps < snaps_before && rows == rows_before
    })
    .await;
    handle.abort();

    let (files_after, snaps_after, rows_after) = observe(&ducklake);
    assert!(
        ok,
        "leased MaintenanceJob must compact + expire; \
         files {files_before}→{files_after}, snaps {snaps_before}→{snaps_after}, \
         rows {rows_before}→{rows_after}"
    );
    assert_eq!(
        rows_after, rows_before,
        "sample rows must survive compact+expire"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_scheduler_short_intervals_compacts_default_scope() {
    let temp = TempDir::new().unwrap();
    let config = maint_test_config(&temp);
    let (files_before, snaps_before, rows_before) =
        seed_closed_day_small_files(&config.ducklake, 200);
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Production entry: wake from config intervals (1s here).
    let handle = start_maintenance_scheduler(&config, None)
        .await
        .expect("scheduler")
        .expect("enabled");

    let ducklake = config.ducklake.clone();
    let ok = wait_until(Duration::from_secs(45), || {
        let (files, snaps, rows) = observe(&ducklake);
        files < files_before && snaps < snaps_before && rows == rows_before
    })
    .await;
    handle.abort();

    let (files_after, snaps_after, rows_after) = observe(&ducklake);
    assert!(
        ok,
        "start_maintenance_scheduler must compact+expire with short intervals; \
         files {files_before}→{files_after}, snaps {snaps_before}→{snaps_after}, \
         rows {rows_before}→{rows_after}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_tenant_leased_maintenance_compacts_each_scope() {
    let temp = TempDir::new().unwrap();
    let config = maint_test_config(&temp);
    let dk_a = ducklake_under(&temp, "tenant_a");
    let dk_b = ducklake_under(&temp, "tenant_b");

    let (files_a0, snaps_a0, rows_a0) = seed_closed_day_small_files(&dk_a, 300);
    let (files_b0, snaps_b0, rows_b0) = seed_closed_day_small_files(&dk_b, 400);
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let executor = MaintenanceExecutor::new(&config, None)
        .await
        .expect("executor");
    let job: Arc<dyn Job> = Arc::new(MaintenanceJob::with_scopes(
        executor,
        Duration::from_millis(100),
        true,
        vec![
            ("tenant_a".into(), dk_a.clone()),
            ("tenant_b".into(), dk_b.clone()),
        ],
    ));
    let leases = Arc::new(MemoryLeaseStore::new());
    let handle =
        spawn_runner(&config.async_jobs, leases as Arc<dyn LeaseStore>, vec![job]).expect("runner");

    let ok = wait_until(Duration::from_secs(45), || {
        let (fa, sa, ra) = observe(&dk_a);
        let (fb, sb, rb) = observe(&dk_b);
        fa < files_a0
            && sa < snaps_a0
            && ra == rows_a0
            && fb < files_b0
            && sb < snaps_b0
            && rb == rows_b0
    })
    .await;
    handle.abort();

    let (fa, sa, ra) = observe(&dk_a);
    let (fb, sb, rb) = observe(&dk_b);
    assert!(
        ok,
        "both tenants must compact+expire; \
         a files {files_a0}→{fa} snaps {snaps_a0}→{sa}; \
         b files {files_b0}→{fb} snaps {snaps_b0}→{sb}"
    );
    assert_eq!(ra, rows_a0);
    assert_eq!(rb, rows_b0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_lease_holder_peer_recovers_and_compacts() {
    let temp = TempDir::new().unwrap();
    let mut config = maint_test_config(&temp);
    config.async_jobs.lease_ttl_seconds = 2;
    config.async_jobs.heartbeat_seconds = 1;
    let (files_before, snaps_before, rows_before) =
        seed_closed_day_small_files(&config.ducklake, 500);
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let leases = Arc::new(MemoryLeaseStore::new());
    // Zombie holds the maintenance lease then "dies" without release (no HB).
    assert!(leases
        .try_acquire("maintenance", "_default", "zombie", Duration::from_secs(2))
        .await
        .unwrap());

    config.async_jobs.instance_id = Some("survivor".into());
    let executor = MaintenanceExecutor::new(&config, None)
        .await
        .expect("executor");
    let job: Arc<dyn Job> = Arc::new(MaintenanceJob::new(
        executor,
        Duration::from_millis(100),
        true,
    ));
    let handle = spawn_runner(
        &config.async_jobs,
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        vec![job],
    )
    .expect("runner");

    // Survivor must wait out zombie TTL (~2s) then compact.
    let ducklake = config.ducklake.clone();
    let ok = wait_until(Duration::from_secs(30), || {
        let (files, snaps, rows) = observe(&ducklake);
        files < files_before && snaps < snaps_before && rows == rows_before
    })
    .await;
    handle.abort();

    let (files_after, snaps_after, rows_after) = observe(&ducklake);
    assert!(
        ok,
        "peer must steal expired lease and finish maintenance; \
         files {files_before}→{files_after}, snaps {snaps_before}→{snaps_after}, \
         rows {rows_before}→{rows_after}"
    );
    assert_eq!(rows_after, rows_before);
}

#[tokio::test]
async fn with_scopes_lists_pinned_tenants_not_default_only() {
    let temp = TempDir::new().unwrap();
    let config = maint_test_config(&temp);
    let dk_a = ducklake_under(&temp, "pin_a");
    let dk_b = ducklake_under(&temp, "pin_b");
    let executor = MaintenanceExecutor::new(&config, None)
        .await
        .expect("executor");
    let job = MaintenanceJob::with_scopes(
        executor,
        Duration::from_millis(100),
        true,
        vec![("pin_a".into(), dk_a), ("pin_b".into(), dk_b)],
    );
    let keys = job.scope_keys().await.expect("scopes");
    assert_eq!(keys, vec!["pin_a".to_string(), "pin_b".to_string()]);
}
