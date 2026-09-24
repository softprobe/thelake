//! Cross-replica job leases. One trait; Postgres in production or memory in tests.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Postgres interval is whole seconds; floor matches `lease_ttl_seconds.max(1)`.
/// Sub-second TTLs are Memory-only (tests).
pub(crate) fn lease_ttl_secs(ttl: Duration) -> i64 {
    ttl.as_secs().max(1) as i64
}

/// Coordination store for `(job_name, scope_key)` single-winner leases.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Race-safe acquire. Returns `true` iff this `holder_id` holds the lease.
    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<bool>;

    /// Extend `lease_until` while still the holder.
    async fn heartbeat(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<()>;

    /// Drop the lease if still held by `holder_id`.
    async fn release(&self, job_name: &str, scope_key: &str, holder_id: &str) -> Result<()>;
}

#[derive(Clone)]
struct MemoryEntry {
    holder_id: String,
    lease_until: Instant,
}

/// In-process lease map — single-node / unit tests.
#[derive(Default)]
pub struct MemoryLeaseStore {
    inner: Mutex<HashMap<(String, String), MemoryEntry>>,
}

impl MemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LeaseStore for MemoryLeaseStore {
    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let mut map = self.inner.lock().await;
        let key = (job_name.to_string(), scope_key.to_string());
        let now = Instant::now();
        let until = now + ttl;
        match map.get(&key) {
            None => {
                map.insert(
                    key,
                    MemoryEntry {
                        holder_id: holder_id.to_string(),
                        lease_until: until,
                    },
                );
                Ok(true)
            }
            Some(e) if e.lease_until < now || e.holder_id == holder_id => {
                let stole = e.lease_until < now && e.holder_id != holder_id;
                map.insert(
                    key,
                    MemoryEntry {
                        holder_id: holder_id.to_string(),
                        lease_until: until,
                    },
                );
                if stole {
                    crate::self_monitoring::record_lease_steal(job_name, scope_key);
                }
                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    async fn heartbeat(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<()> {
        let mut map = self.inner.lock().await;
        let key = (job_name.to_string(), scope_key.to_string());
        match map.get_mut(&key) {
            Some(e) if e.holder_id == holder_id => {
                e.lease_until = Instant::now() + ttl;
                Ok(())
            }
            Some(_) => Err(anyhow!("heartbeat: not holder")),
            None => Err(anyhow!("heartbeat: lease missing")),
        }
    }

    async fn release(&self, job_name: &str, scope_key: &str, holder_id: &str) -> Result<()> {
        let mut map = self.inner.lock().await;
        let key = (job_name.to_string(), scope_key.to_string());
        match map.get(&key) {
            Some(e) if e.holder_id == holder_id => {
                map.remove(&key);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Catalog-Postgres lease rows in `{registry}.thelake_job_lease`.
///
/// Expiry and renew/steal decisions use **Postgres `now()` only** — not the
/// application node's clock — so replica clock skew cannot make one node
/// believe it still holds a lease the DB already considers stealable.
/// Delayed heartbeat/renewal after `lease_until` is a fair race: another
/// holder may win the UPSERT; configure `lease_ttl_seconds` ≫ typical pass
/// latency so heartbeats land while the row is still valid.
pub struct PostgresLeaseStore {
    pool: Pool,
    /// Qualified `"schema".thelake_job_lease`
    table: String,
}

impl PostgresLeaseStore {
    pub fn new(pool: Pool, registry_schema: &str) -> Self {
        let table = format!(
            "{}.thelake_job_lease",
            crate::runtime_engine::quote_pg_ident(registry_schema)
        );
        Self { pool, table }
    }

    pub(crate) fn from_resolver(resolver: &crate::runtime_engine::DuckLakeScopeResolver) -> Self {
        Self::new(resolver.pool().clone(), resolver.registry_schema())
    }

    /// Lease store backed by the process catalog registry.
    pub fn from_engines(engines: &crate::runtime_engine::RuntimeEngineManager) -> Self {
        engines.lease_store()
    }
}

#[async_trait]
impl LeaseStore for PostgresLeaseStore {
    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let client = self.pool.get().await?;
        let ttl_secs = lease_ttl_secs(ttl);
        // Single race-safe UPSERT (design §4). Steal metrics are Memory-only —
        // peek+FOR UPDATE was dropped to avoid a second SQL for a counter.
        let sql = format!(
            r#"
INSERT INTO {table} (job_name, scope_key, holder_id, lease_until, heartbeat_at)
VALUES ($1, $2, $3, now() + ($4::bigint * INTERVAL '1 second'), now())
ON CONFLICT (job_name, scope_key) DO UPDATE SET
  holder_id = EXCLUDED.holder_id,
  lease_until = EXCLUDED.lease_until,
  heartbeat_at = EXCLUDED.heartbeat_at
WHERE thelake_job_lease.lease_until < now()
   OR thelake_job_lease.holder_id = EXCLUDED.holder_id
RETURNING holder_id
"#,
            table = self.table
        );
        let row = client
            .query_opt(&sql, &[&job_name, &scope_key, &holder_id, &ttl_secs])
            .await?;
        Ok(matches!(row, Some(r) if r.get::<_, String>(0) == holder_id))
    }

    async fn heartbeat(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        let ttl_secs = lease_ttl_secs(ttl);
        let sql = format!(
            r#"
UPDATE {table}
SET lease_until = now() + ($4::bigint * INTERVAL '1 second'),
    heartbeat_at = now()
WHERE job_name = $1 AND scope_key = $2 AND holder_id = $3
"#,
            table = self.table
        );
        let n = client
            .execute(&sql, &[&job_name, &scope_key, &holder_id, &ttl_secs])
            .await?;
        if n == 0 {
            return Err(anyhow!("heartbeat: not holder or missing"));
        }
        Ok(())
    }

    async fn release(&self, job_name: &str, scope_key: &str, holder_id: &str) -> Result<()> {
        let client = self.pool.get().await?;
        let sql = format!(
            r#"DELETE FROM {table} WHERE job_name = $1 AND scope_key = $2 AND holder_id = $3"#,
            table = self.table
        );
        let _ = client
            .execute(&sql, &[&job_name, &scope_key, &holder_id])
            .await?;
        Ok(())
    }
}
