//! Cross-replica job leases. One trait; Postgres (multi-replica) or memory (sqlite/tests).

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

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

/// In-process lease map — sqlite / single-node / unit tests.
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

    pub fn from_resolver(resolver: &crate::runtime_engine::DuckLakeScopeResolver) -> Self {
        Self::new(resolver.pool().clone(), resolver.registry_schema())
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
        let mut client = self.pool.get().await?;
        // Postgres interval is whole seconds; floor matches spawn_runner's
        // `lease_ttl_seconds.max(1)`. Sub-second TTLs are Memory-only (tests).
        let ttl_secs = ttl.as_secs().max(1) as i64;
        // Serialize peek + UPSERT so steal metrics see the row that the UPSERT raced.
        let tx = client.transaction().await?;
        let peek_sql = format!(
            r#"SELECT holder_id, lease_until < now() AS expired
FROM {table} WHERE job_name = $1 AND scope_key = $2
FOR UPDATE"#,
            table = self.table
        );
        let prev = tx.query_opt(&peek_sql, &[&job_name, &scope_key]).await?;
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
        let row = tx
            .query_opt(&sql, &[&job_name, &scope_key, &holder_id, &ttl_secs])
            .await?;
        tx.commit().await?;
        let won = matches!(row, Some(r) if r.get::<_, String>(0) == holder_id);
        if won {
            if let Some(prev) = prev {
                let prev_holder: String = prev.get(0);
                let expired: bool = prev.get(1);
                if expired && prev_holder != holder_id {
                    crate::self_monitoring::record_lease_steal(job_name, scope_key);
                }
            }
        }
        Ok(won)
    }

    async fn heartbeat(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        // Same whole-second floor as try_acquire.
        let ttl_secs = ttl.as_secs().max(1) as i64;
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
