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

/// Fencing identity for one successful lease acquisition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseToken {
    pub holder_id: String,
    pub epoch: i64,
}

/// Coordination store for `(job_name, scope_key)` single-winner leases.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Acquire or renew a lease and return its fencing token.
    async fn acquire_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseToken>>;

    /// Extend a lease only while the exact fencing token remains current.
    async fn heartbeat_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
        ttl: Duration,
    ) -> Result<()>;

    /// Expire (but retain) the row only while the exact token remains current.
    async fn release_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
    ) -> Result<()>;

    /// Race-safe compatibility helper. Production jobs should use `acquire_lease`.
    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        Ok(self
            .acquire_lease(job_name, scope_key, holder_id, ttl)
            .await?
            .is_some())
    }
}

#[derive(Clone)]
struct MemoryEntry {
    holder_id: String,
    epoch: i64,
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
    async fn acquire_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseToken>> {
        let mut map = self.inner.lock().await;
        let key = (job_name.to_string(), scope_key.to_string());
        let now = Instant::now();
        let until = now + ttl;
        let epoch = match map.get(&key) {
            None => 1,
            Some(e) if e.lease_until < now => e.epoch + 1,
            Some(e) if e.holder_id == holder_id => e.epoch,
            Some(_) => return Ok(None),
        };
        let stole = map.get(&key).is_some_and(|e| e.lease_until < now);
        map.insert(
            key,
            MemoryEntry {
                holder_id: holder_id.to_string(),
                epoch,
                lease_until: until,
            },
        );
        if stole {
            crate::self_monitoring::record_lease_steal(job_name, scope_key);
        }
        Ok(Some(LeaseToken {
            holder_id: holder_id.to_string(),
            epoch,
        }))
    }

    async fn heartbeat_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
        ttl: Duration,
    ) -> Result<()> {
        let mut map = self.inner.lock().await;
        let key = (job_name.to_string(), scope_key.to_string());
        match map.get_mut(&key) {
            Some(e)
                if e.holder_id == token.holder_id
                    && e.epoch == token.epoch
                    && e.lease_until > Instant::now() =>
            {
                e.lease_until = Instant::now() + ttl;
                Ok(())
            }
            _ => Err(anyhow!("heartbeat: lease token lost or expired")),
        }
    }

    async fn release_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
    ) -> Result<()> {
        let mut map = self.inner.lock().await;
        let key = (job_name.to_string(), scope_key.to_string());
        if let Some(e) = map.get_mut(&key) {
            if e.holder_id == token.holder_id && e.epoch == token.epoch {
                e.lease_until = Instant::now();
            }
        }
        Ok(())
    }

    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        Ok(self
            .acquire_lease(job_name, scope_key, holder_id, ttl)
            .await?
            .is_some())
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
#[derive(Clone)]
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
    async fn acquire_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseToken>> {
        let client = self.pool.get().await?;
        let ttl_secs = lease_ttl_secs(ttl);
        let sql = format!(
            r#"
INSERT INTO {table} AS current_lease (job_name, scope_key, holder_id, epoch, lease_until, heartbeat_at)
VALUES ($1, $2, $3, 1, now() + ($4::bigint * INTERVAL '1 second'), now())
ON CONFLICT (job_name, scope_key) DO UPDATE SET
  holder_id = EXCLUDED.holder_id,
  epoch = CASE WHEN current_lease.lease_until <= now() THEN current_lease.epoch + 1 ELSE current_lease.epoch END,
  lease_until = EXCLUDED.lease_until,
  heartbeat_at = EXCLUDED.heartbeat_at
WHERE current_lease.lease_until <= now() OR current_lease.holder_id = EXCLUDED.holder_id
RETURNING holder_id, epoch
"#,
            table = self.table
        );
        let row = client
            .query_opt(&sql, &[&job_name, &scope_key, &holder_id, &ttl_secs])
            .await?;
        Ok(row.map(|r| LeaseToken {
            holder_id: r.get(0),
            epoch: r.get(1),
        }))
    }

    async fn heartbeat_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
        ttl: Duration,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        let ttl_secs = lease_ttl_secs(ttl);
        let sql = format!(
            r#"UPDATE {table}
SET lease_until = now() + ($4::bigint * INTERVAL '1 second'), heartbeat_at = now()
WHERE job_name = $1 AND scope_key = $2 AND holder_id = $3 AND epoch = $5 AND lease_until > now()"#,
            table = self.table
        );
        let n = client
            .execute(
                &sql,
                &[
                    &job_name,
                    &scope_key,
                    &token.holder_id,
                    &ttl_secs,
                    &token.epoch,
                ],
            )
            .await?;
        if n == 0 {
            return Err(anyhow!("heartbeat: lease token lost or expired"));
        }
        Ok(())
    }

    async fn release_lease(
        &self,
        job_name: &str,
        scope_key: &str,
        token: &LeaseToken,
    ) -> Result<()> {
        let client = self.pool.get().await?;
        let sql = format!(
            "UPDATE {table} SET lease_until = now() WHERE job_name = $1 AND scope_key = $2 AND holder_id = $3 AND epoch = $4",
            table = self.table
        );
        client
            .execute(
                &sql,
                &[&job_name, &scope_key, &token.holder_id, &token.epoch],
            )
            .await?;
        Ok(())
    }

    async fn try_acquire(
        &self,
        job_name: &str,
        scope_key: &str,
        holder_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        Ok(self
            .acquire_lease(job_name, scope_key, holder_id, ttl)
            .await?
            .is_some())
    }
}
