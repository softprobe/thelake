//! Size-1 per-physical-scope pool of Maintenance-kind DuckDB connections.
//!
//! Writers and query workers keep their own pools (different thread/memory caps).
//! ATTACH once; recreate on poison. Never hold a `Connection` across `.await`.

use crate::config::Config;
use crate::storage::ducklake::{DuckLakeAccess, PhysicalScope};
use anyhow::{anyhow, Result};
use duckdb::Connection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) struct MaintenanceConnPool {
    config: Config,
    slots: Mutex<HashMap<String, Arc<Mutex<Connection>>>>,
}

impl MaintenanceConnPool {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
            slots: Mutex::new(HashMap::new()),
        }
    }

    fn open_attached(config: &Config, physical: &PhysicalScope) -> Result<Connection> {
        let access = DuckLakeAccess::Physical(physical.clone());
        let factory = crate::storage::ducklake::DuckLakeSessionFactory::new(config);
        let conn = factory
            .open(
                &access,
                crate::storage::ducklake::DuckLakeSessionKind::Maintenance,
            )
            .map_err(|e| anyhow!("maintenance DuckDB open: {e}"))?;
        factory
            .attach(&conn, &access)
            .map_err(|e| anyhow!("maintenance DuckDB attach: {e}"))?;
        let registry_dsn = config
            .ducklake
            .metadata_path
            .strip_prefix("postgres:")
            .unwrap_or(&config.ducklake.metadata_path);
        let registry_target = crate::sql::literal::sql_string_literal(registry_dsn);
        conn.execute_batch(&format!(
            "ATTACH {registry_target} AS __thelake_registry (TYPE postgres);"
        ))
        .map_err(|e| anyhow!("maintenance registry attach: {e}"))?;
        Ok(conn)
    }

    /// Borrow the pooled connection.
    ///
    /// Returns `(result, cold_open, open_or_checkout_elapsed)`.
    /// On catalog/connection poison, drops the slot so the next borrow cold-opens.
    pub(crate) fn with_conn<R>(
        &self,
        physical: &PhysicalScope,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<(R, bool, Duration)> {
        let key = physical.writer_pool_key();
        let started = Instant::now();
        let slot = {
            let mut guard = self
                .slots
                .lock()
                .map_err(|_| anyhow!("maintenance conn pool lock poisoned"))?;
            if let Some(existing) = guard.get(&key) {
                (Arc::clone(existing), false)
            } else {
                let conn = Self::open_attached(&self.config, physical)?;
                let slot = Arc::new(Mutex::new(conn));
                guard.insert(key.clone(), Arc::clone(&slot));
                (slot, true)
            }
        };
        let (slot, cold) = slot;
        let checkout = started.elapsed();
        let guard = slot
            .lock()
            .map_err(|_| anyhow!("maintenance connection lock poisoned"))?;
        match f(&guard) {
            Ok(v) => Ok((v, cold, checkout)),
            Err(err) => {
                let msg = err.to_string().to_lowercase();
                if msg.contains("connection")
                    || msg.contains("catalog")
                    || msg.contains("detach")
                    || msg.contains("not open")
                {
                    drop(guard);
                    if let Ok(mut map) = self.slots.lock() {
                        map.remove(&key);
                    }
                }
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pool_module_is_maintenance_only() {
        let src = include_str!("maint_conn_pool.rs");
        let prod = src.split("#[cfg(test)]").next().expect("production");
        assert!(prod.contains("DuckLakeSessionKind::Maintenance"));
        assert!(
            !prod.contains("DuckLakeSessionKind::Writer")
                && !prod.contains("DuckLakeSessionKind::Query"),
            "maintenance pool must not open Writer/Query sessions"
        );
    }
}
