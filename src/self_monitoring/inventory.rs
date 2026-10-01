//! Best-effort DuckLake file inventory scrape → gauge_store.
//!
//! Uses a one-shot uninstrumented DuckDB connection so metadata SQL does not
//! pollute customer query latency / slow-query series or contend on workers.

use crate::api::AppState;
use crate::compaction::maintenance_table_names;
use crate::sql::maintenance::live_file_sizes_sql;
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

/// Periodically refresh inventory + process gauges for cached engines.
pub fn spawn_inventory_loop(state: AppState, interval_secs: u64) {
    // Floor at 60s: sub-minute open+attach scrapes pegged one core once tenants
    // were cached (demo Softprobe sat at ~100% CPU with no OTLP/Grafana).
    let interval = std::time::Duration::from_secs(interval_secs.max(60));
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
            // Interval fires immediately; skip DuckDB attach on that first tick so
            // Softprobe is not pegged at startup / after every process restart.
            static SKIP_FIRST: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(true);
            if SKIP_FIRST.swap(false, std::sync::atomic::Ordering::Relaxed) {
                continue;
            }
            let tenants = state.engines.list_cached_workspace_ids();
            for tenant in tenants {
                if tenant.trim().is_empty() {
                    continue;
                }
                // Ops scope inventory feeds the same writer; skip the feedback loop.
                if crate::self_monitoring::is_reserved_workspace_id(&tenant) {
                    continue;
                }
                let Ok(engine) = state.engines.engine_for(&tenant).await else {
                    continue;
                };
                // One attach per tenant (not per SQL) — each uninstrumented query
                // used to open+INSTALL+ATTACH and dominated Softprobe CPU.
                scrape_tenant(engine.as_ref()).await;
            }
        }
    });
}

async fn scrape_tenant(engine: &crate::runtime_engine::RuntimeEngine) {
    let tables = maintenance_table_names();
    let query = engine.query_engine();
    let catalog = query.catalog_alias().to_string();
    let tenant = engine.workspace_id().to_string();
    let sqls: Vec<String> = tables
        .iter()
        .map(|table| live_file_sizes_sql(&catalog, table))
        .collect();
    let sql_refs: Vec<&str> = sqls.iter().map(|s| s.as_str()).collect();
    let results = match query.execute_queries_uninstrumented(sql_refs).await {
        Ok(r) => r,
        Err(err) => {
            warn!(tenant = %tenant, "inventory scrape failed: {err}");
            return;
        }
    };
    for (idx, table) in tables.into_iter().enumerate() {
        let size_res = results.get(idx);
        let mut live_files = 0usize;
        let mut live_bytes = 0u64;
        let mut counts = [
            (BUCKET_LT_1MB, 0u64),
            (BUCKET_1_8MB, 0u64),
            (BUCKET_8_64MB, 0u64),
            (BUCKET_GTE_64MB, 0u64),
        ];
        match size_res {
            Some(Ok(res)) => {
                for row in &res.rows {
                    if let Some(v) = row.first() {
                        let bytes = json_u64(v);
                        live_files += 1;
                        live_bytes += bytes;
                        let b = size_bucket(bytes);
                        for (name, c) in counts.iter_mut() {
                            if *name == b {
                                *c += 1;
                            }
                        }
                    }
                }
            }
            Some(Err(err)) => {
                warn!(tenant = %tenant, table, "inventory size buckets failed: {err}");
            }
            None => {}
        }
        for (bucket, c) in counts {
            gauge_store::set_size_bucket(&tenant, table, bucket, c);
        }
        gauge_store::set_table_inventory(
            &tenant,
            table,
            TableInventory {
                live_files: live_files as u64,
                live_bytes,
            },
        );
    }
}
