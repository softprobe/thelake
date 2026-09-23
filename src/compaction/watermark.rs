//! Per physical-scope / table compaction watermark (Postgres app store).

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;

use crate::sql::maintenance::{
    compaction_watermark_advance_sql, compaction_watermark_create_table_sql,
    compaction_watermark_get_sql, compaction_watermark_insert_fence_sql,
};

pub(crate) struct WatermarkStore {
    pool: Pool,
    registry_schema: String,
}

impl WatermarkStore {
    pub(crate) fn new(pool: Pool, registry_schema: String) -> Self {
        Self {
            pool,
            registry_schema,
        }
    }

    pub(crate) async fn ensure_table(&self) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                &compaction_watermark_create_table_sql(&self.registry_schema),
                &[],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn get(
        &self,
        scope_key: &str,
        table: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        self.ensure_table().await?;
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                &compaction_watermark_get_sql(&self.registry_schema),
                &[&scope_key, &table],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    /// Insert fence only if absent; returns `(watermark, inserted_new)`.
    pub(crate) async fn ensure_fence(
        &self,
        scope_key: &str,
        table: &str,
        fence: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, bool)> {
        self.ensure_table().await?;
        if let Some(existing) = self.get(scope_key, table).await? {
            return Ok((existing, false));
        }
        let client = self.pool.get().await?;
        let inserted = client
            .execute(
                &compaction_watermark_insert_fence_sql(&self.registry_schema),
                &[&scope_key, &table, &fence],
            )
            .await?;
        let wm = self
            .get(scope_key, table)
            .await?
            .ok_or_else(|| anyhow!("compaction watermark missing after fence insert"))?;
        Ok((wm, inserted > 0))
    }

    pub(crate) async fn advance(
        &self,
        scope_key: &str,
        table: &str,
        watermark: DateTime<Utc>,
    ) -> Result<()> {
        self.ensure_table().await?;
        let client = self.pool.get().await?;
        let n = client
            .execute(
                &compaction_watermark_advance_sql(&self.registry_schema),
                &[&scope_key, &table, &watermark],
            )
            .await?;
        if n == 0 {
            return Err(anyhow!(
                "compaction watermark advance: no row for {scope_key}/{table}"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
    use std::time::Duration;
    use tokio_postgres::NoTls;

    async fn try_watermark_store(schema: &str) -> Option<WatermarkStore> {
        let mut pg = tokio_postgres::Config::new();
        pg.host("localhost");
        pg.port(5432);
        pg.dbname("ducklake");
        pg.user("ducklake");
        pg.password("ducklake");
        let mgr = Manager::from_config(
            pg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr).max_size(4).build().ok()?;
        let client = match tokio::time::timeout(Duration::from_secs(2), pool.get()).await {
            Ok(Ok(c)) => c,
            _ => return None,
        };
        let q = crate::runtime_engine::quote_pg_ident(schema);
        client
            .execute(&format!("CREATE SCHEMA IF NOT EXISTS {q}"), &[])
            .await
            .ok()?;
        client
            .execute(
                &format!("DROP TABLE IF EXISTS {q}.compaction_watermark"),
                &[],
            )
            .await
            .ok()?;
        Some(WatermarkStore::new(pool, schema.to_string()))
    }

    #[tokio::test]
    #[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
    async fn postgres_compaction_watermark_fence_then_advance() {
        let store = try_watermark_store("thelake_wm_ut")
            .await
            .expect("ducklake-postgres required (make setup)");
        let fence = Utc.with_ymd_and_hms(2026, 9, 1, 12, 0, 0).unwrap();
        let (wm, inserted) = store
            .ensure_fence("scope-a", "traces", fence)
            .await
            .expect("fence");
        assert!(inserted, "first fence must insert");
        assert_eq!(wm, fence);

        let later = Utc.with_ymd_and_hms(2026, 9, 2, 0, 0, 0).unwrap();
        let (wm2, inserted2) = store
            .ensure_fence("scope-a", "traces", later)
            .await
            .expect("idempotent fence");
        assert!(!inserted2, "second fence must not insert");
        assert_eq!(wm2, fence, "existing watermark must be retained");

        store
            .advance("scope-a", "traces", later)
            .await
            .expect("advance");
        assert_eq!(
            store.get("scope-a", "traces").await.unwrap(),
            Some(later)
        );
    }

    #[tokio::test]
    #[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
    async fn postgres_compaction_watermark_advance_without_row_fails() {
        let store = try_watermark_store("thelake_wm_advance_miss")
            .await
            .expect("ducklake-postgres required (make setup)");
        store.ensure_table().await.expect("create table");
        let err = store
            .advance(
                "missing-scope",
                "traces",
                Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
            )
            .await
            .expect_err("advance with no row must fail");
        assert!(
            err.to_string().contains("no row"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    #[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
    async fn postgres_compaction_watermark_scopes_are_independent() {
        let store = try_watermark_store("thelake_wm_scopes")
            .await
            .expect("ducklake-postgres required (make setup)");
        let a = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
        let b = Utc.with_ymd_and_hms(2026, 9, 2, 0, 0, 0).unwrap();
        let (wm_a, _) = store.ensure_fence("scope-a", "traces", a).await.unwrap();
        let (wm_b, _) = store.ensure_fence("scope-b", "traces", b).await.unwrap();
        assert_eq!(wm_a, a);
        assert_eq!(wm_b, b);
        assert_ne!(
            store.get("scope-a", "traces").await.unwrap(),
            store.get("scope-b", "traces").await.unwrap()
        );
    }
}
