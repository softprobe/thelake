//! Postgres integration tests for session_summary DDL + dirty UPSERT.
//! Run via `make test-lease-pg` / `make test-e2e` (`cargo test --lib postgres_ -- --ignored`).

use super::*;
use crate::ingest_engine::maybe_after_traces_commit;
use crate::session_summary::test_span::span_at;
use chrono::{TimeZone, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::time::Duration;
use tokio_postgres::NoTls;

async fn try_pg_pool(schema: &str) -> Option<Pool> {
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
    ensure_session_summary_tables(&client, schema).await.ok()?;
    let q = crate::runtime_engine::quote_pg_ident(schema);
    client
        .execute(
            &format!("TRUNCATE {q}.session_summary, {q}.session_summary_dirty"),
            &[],
        )
        .await
        .ok()?;
    Some(pool)
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_ensure_idempotent() {
    let schema = "thelake_ss_ensure";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let client = pool.get().await.expect("client");
    ensure_session_summary_tables(&client, schema)
        .await
        .expect("second ensure");
    let n: i64 = client
        .query_one(
            &format!(
                "SELECT count(*)::bigint FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name IN ('session_summary', 'session_summary_dirty')"
            ),
            &[&schema],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 2, "both tables present");
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_dirty_upsert_merge() {
    let schema = "thelake_ss_dirty";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");

    dirty
        .upsert_dirty(&fold_dirty_hints(&[
            span_at("s1", 10),
            span_at("s2", 20),
            span_at("s1", 5),
        ]))
        .await
        .expect("first upsert");

    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 1), span_at("s1", 50)]))
        .await
        .expect("second upsert expands bounds");

    // Narrower third batch must not shrink bounds (LEAST/GREATEST).
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 12), span_at("s1", 15)]))
        .await
        .expect("third upsert keeps outer bounds");

    let client = pool.get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    let rows = client
        .query(
            &format!(
                "SELECT session_id, min_ts, max_ts FROM {q}.session_summary_dirty ORDER BY session_id"
            ),
            &[],
        )
        .await
        .expect("select dirty");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "s1");
    assert_eq!(
        rows[0].get::<_, chrono::DateTime<Utc>>(1),
        Utc.timestamp_opt(1, 0).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, chrono::DateTime<Utc>>(2),
        Utc.timestamp_opt(50, 0).unwrap()
    );
    assert_eq!(rows[1].get::<_, String>(0), "s2");

    let summary_n: i64 = client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {q}.session_summary"),
            &[],
        )
        .await
        .expect("summary count")
        .get(0);
    assert_eq!(summary_n, 0, "session_summary stays empty until reduce");
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_mark_after_commit_writes_dirty() {
    let schema = "thelake_ss_mark";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    dirty
        .mark_after_traces_commit(&[span_at("a", 10), span_at("b", 20), span_at("a", 5)])
        .await;
    let client = pool.get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    let rows = client
        .query(
            &format!(
                "SELECT session_id, min_ts, max_ts FROM {q}.session_summary_dirty ORDER BY session_id"
            ),
            &[],
        )
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(
        rows[0].get::<_, chrono::DateTime<Utc>>(1),
        Utc.timestamp_opt(5, 0).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, chrono::DateTime<Utc>>(2),
        Utc.timestamp_opt(10, 0).unwrap()
    );
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_maybe_after_traces_commit_write_err_skips_dirty() {
    let schema = "thelake_ss_skip";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    let hints = fold_dirty_hints(&[span_at("s1", 1)]);
    maybe_after_traces_commit(false, "t1", 1, true, &hints, Some(&dirty)).await;
    let client = pool.get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    let n: i64 = client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {q}.session_summary_dirty"),
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 0, "write Err must not touch dirty");

    maybe_after_traces_commit(true, "t1", 1, true, &hints, Some(&dirty)).await;
    let n2: i64 = client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {q}.session_summary_dirty"),
            &[],
        )
        .await
        .expect("count2")
        .get(0);
    assert_eq!(n2, 1);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_dirty_err_does_not_propagate() {
    let schema = "thelake_ss_ok";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool, "thelake_ss_missing_schema_xyz", "t1");
    // Must return (not panic); ingest path treats dirty as best-effort.
    dirty.mark_after_traces_commit(&[span_at("s1", 1)]).await;
}
