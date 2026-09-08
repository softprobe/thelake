//! Best-effort DuckLake file inventory scrape → gauge_store.
//!
//! Uses a one-shot uninstrumented DuckDB connection so metadata SQL does not
//! pollute customer query latency / slow-query series or contend on workers.

use crate::api::AppState;
use crate::compaction::executor::maintenance_table_names;
use crate::compaction::twcs::{
    live_file_sizes_sql, open_day_files_for_merge, partition_live_file_stats_sql,
    PartitionFileStats,
};
use chrono::{NaiveDate, Utc};
use serde_json::Value;
use tracing::warn;

use super::gauge_store::{self, TableInventory};
use super::size_bucket::{
    size_bucket, BUCKET_1_8MB, BUCKET_8_64MB, BUCKET_GTE_64MB, BUCKET_LT_1MB,
};

fn json_u64(v: &Value) -> u64 {
    match v {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|i| i.max(0) as u64))
            .unwrap_or(0),
        Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn json_date(v: &Value, fallback: NaiveDate) -> NaiveDate {
    let s = match v {
        Value::String(s) => s.as_str(),
        _ => return fallback,
    };
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(&s[..10.min(s.len())], "%Y-%m-%d"))
        .unwrap_or(fallback)
}

async fn scrape_table(engine: &crate::runtime_engine::RuntimeEngine, table: &str) {
    let catalog = engine.query.catalog_alias();
    let tenant = engine.tenant_id.as_str();
    let today = Utc::now().date_naive();
    let mut live_files = 0usize;
    let mut live_bytes = 0u64;
    let mut partitions: Vec<PartitionFileStats> = Vec::new();

    match engine
        .query
        .execute_query_uninstrumented(&partition_live_file_stats_sql(catalog, table))
        .await
    {
        Ok(res) => {
            for row in &res.rows {
                if row.len() < 3 {
                    continue;
                }
                let count = json_u64(&row[1]) as usize;
                let bytes = json_u64(&row[2]);
                live_files += count;
                live_bytes += bytes;
                partitions.push(PartitionFileStats {
                    record_date: json_date(&row[0], today),
                    live_file_count: count,
                    total_bytes: bytes,
                });
            }
        }
        Err(err) => {
            warn!(tenant, table, "inventory partition stats failed: {err}");
        }
    }

    let open_day = open_day_files_for_merge(&partitions, today, Some(live_files));
    gauge_store::set_table_inventory(
        tenant,
        table,
        TableInventory {
            live_files: live_files as u64,
            live_bytes,
            open_day_live_files: open_day as u64,
        },
    );

    let mut counts = [
        (BUCKET_LT_1MB, 0u64),
        (BUCKET_1_8MB, 0u64),
        (BUCKET_8_64MB, 0u64),
        (BUCKET_GTE_64MB, 0u64),
    ];
    match engine
        .query
        .execute_query_uninstrumented(&live_file_sizes_sql(catalog, table))
        .await
    {
        Ok(res) => {
            for row in &res.rows {
                if let Some(v) = row.first() {
                    let b = size_bucket(json_u64(v));
                    for (name, c) in counts.iter_mut() {
                        if *name == b {
                            *c += 1;
                        }
                    }
                }
            }
        }
        Err(err) => {
            warn!(tenant, table, "inventory size buckets failed: {err}");
        }
    }
    for (bucket, c) in counts {
        gauge_store::set_size_bucket(tenant, table, bucket, c);
    }
}

/// Periodically refresh inventory + process gauges for cached engines.
pub fn spawn_inventory_loop(state: AppState, interval_secs: u64) {
    let interval = std::time::Duration::from_secs(interval_secs.max(1));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            super::instruments::refresh_process_gauges();
            gauge_store::WRITER_POOL_SIZE.store(
                state.engines.config().ducklake.writer_pool_size,
                std::sync::atomic::Ordering::Relaxed,
            );
            // When flush-through (no coalesce), pending stays 0.
            if state.engines.config().ingest.flush_interval_seconds == 0 {
                gauge_store::INGEST_PENDING_BATCHES.store(0, std::sync::atomic::Ordering::Relaxed);
            }
            let tenants = state.engines.list_cached_tenant_ids();
            for tenant in tenants {
                if tenant.trim().is_empty() {
                    continue;
                }
                let Ok(engine) = state.engines.engine_for(&tenant).await else {
                    continue;
                };
                for table in maintenance_table_names() {
                    scrape_table(engine.as_ref(), table).await;
                }
            }
        }
    });
}
