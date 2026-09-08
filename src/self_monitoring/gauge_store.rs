//! In-process gauge snapshot for ObservableGauge callbacks (inventory + saturation).

use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, AtomicUsize};

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableKey {
    pub tenant: String,
    pub table: String,
}

#[derive(Clone, Default)]
pub struct TableInventory {
    pub live_files: u64,
    pub live_bytes: u64,
    pub open_day_live_files: u64,
}

/// size_bucket → count
pub type SizeBuckets = DashMap<(String, String, String), u64>;

pub static TABLE_INV: Lazy<DashMap<TableKey, TableInventory>> = Lazy::new(DashMap::new);
pub static SIZE_BUCKETS: Lazy<SizeBuckets> = Lazy::new(DashMap::new);
pub static COMPACTION_FILES_BEFORE: Lazy<DashMap<(String, String, String), u64>> =
    Lazy::new(DashMap::new);
pub static COMPACTION_FILES_AFTER: Lazy<DashMap<(String, String, String), u64>> =
    Lazy::new(DashMap::new);

pub static QUERY_WORKERS: AtomicUsize = AtomicUsize::new(0);
pub static QUERY_WORKERS_BUSY: AtomicUsize = AtomicUsize::new(0);
pub static INGEST_PENDING_BATCHES: AtomicUsize = AtomicUsize::new(0);
pub static WRITER_POOL_SIZE: AtomicUsize = AtomicUsize::new(0);

/// Concurrent-safe pending-batch depth (shared across coalesce buffers / tenants).
pub fn add_ingest_pending(n: usize) {
    if n > 0 {
        INGEST_PENDING_BATCHES.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }
}

pub fn sub_ingest_pending(n: usize) {
    if n == 0 {
        return;
    }
    let _ = INGEST_PENDING_BATCHES.fetch_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |cur| Some(cur.saturating_sub(n)),
    );
}

pub static PROCESS_RSS: AtomicU64 = AtomicU64::new(0);
pub static PROCESS_VSIZE: AtomicU64 = AtomicU64::new(0);
pub static PROCESS_CPU_MILLI: AtomicU64 = AtomicU64::new(0); // cpu% * 10 (ratio*1000)
pub static PROCESS_THREADS: AtomicU64 = AtomicU64::new(0);
pub static PROCESS_DISK_READ: AtomicU64 = AtomicU64::new(0);
pub static PROCESS_DISK_WRITE: AtomicU64 = AtomicU64::new(0);

pub fn set_table_inventory(tenant: &str, table: &str, inv: TableInventory) {
    TABLE_INV.insert(
        TableKey {
            tenant: tenant.to_string(),
            table: table.to_string(),
        },
        inv,
    );
}

pub fn set_size_bucket(tenant: &str, table: &str, bucket: &str, count: u64) {
    SIZE_BUCKETS.insert(
        (tenant.to_string(), table.to_string(), bucket.to_string()),
        count,
    );
}

pub fn set_compaction_files(tenant: &str, table: &str, day_kind: &str, before: u64, after: u64) {
    let k = (tenant.to_string(), table.to_string(), day_kind.to_string());
    COMPACTION_FILES_BEFORE.insert(k.clone(), before);
    COMPACTION_FILES_AFTER.insert(k, after);
}
