//! Postgres integration tests for session_summary DDL + dirty UPSERT.
//! Run via `make test-lease-pg` / `make test-e2e` (`cargo test --lib postgres_ -- --ignored`).

use super::*;
use crate::ingest_engine::maybe_after_traces_commit;
use crate::session_summary::reduce::{
    ack_dirty, claim_dirty, dirty_depth, publish_claimed_summary_rows, upsert_summary_rows,
    SummaryRow,
};
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
    let q = crate::runtime_engine::quote_pg_ident(schema);
    client.execute(&format!("ALTER TABLE {q}.session_summary_dirty DROP COLUMN IF EXISTS claim_holder, DROP COLUMN IF EXISTS claim_until"), &[])
        .await.expect("simulate pre-claims table");
    ensure_session_summary_tables(&client, schema)
        .await
        .expect("upgrade existing dirty table");
    let claim_columns: i64 = client.query_one(
        "SELECT count(*)::bigint FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'session_summary_dirty' AND column_name IN ('claim_holder', 'claim_until', 'generation')",
        &[&schema],
    ).await.expect("claim columns").get(0);
    assert_eq!(
        claim_columns, 3,
        "existing table gets claim and generation columns"
    );
    let n: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name IN ('session_summary', 'session_summary_dirty')",
            &[&schema],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(n, 2, "both tables present");
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_timestamp_cutover_preserves_and_requeues_rows() {
    let schema = "thelake_ss_timestamp_cutover";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let client = pool.get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    client
        .batch_execute(&format!(
            "DROP TABLE {q}.session_summary_dirty, {q}.session_summary;\
             CREATE TABLE {q}.session_summary (\
               session_id TEXT PRIMARY KEY, start_time TIMESTAMPTZ NOT NULL, end_time TIMESTAMPTZ,\
               observation_count BIGINT NOT NULL DEFAULT 0, error_count BIGINT NOT NULL DEFAULT 0,\
               input_tokens BIGINT, output_tokens BIGINT, total_tokens BIGINT, total_cost DOUBLE PRECISION,\
               agent_name TEXT, user_id TEXT, model_name TEXT, updated_at TIMESTAMPTZ NOT NULL);\
             CREATE TABLE {q}.session_summary_dirty (\
               session_id TEXT PRIMARY KEY, min_ts TIMESTAMPTZ NOT NULL, max_ts TIMESTAMPTZ NOT NULL,\
               updated_at TIMESTAMPTZ NOT NULL, generation BIGINT NOT NULL DEFAULT 1,\
               claim_holder TEXT, claim_until TIMESTAMPTZ);\
             INSERT INTO {q}.session_summary (session_id, start_time, end_time, updated_at)\
               VALUES ('legacy-ns', '2024-01-01T00:00:00.123456Z', '2024-01-01T00:00:00.654321Z', now());\
             INSERT INTO {q}.session_summary_dirty (session_id, min_ts, max_ts, updated_at)\
               VALUES ('pending-only', '2024-01-02T00:00:00.123456Z', '2024-01-02T00:00:00.654321Z', now());"
        ))
        .await
        .expect("create legacy schema fixture");

    ensure_session_summary_tables(&client, schema)
        .await
        .expect("run timestamp cutover");

    let row = client
        .query_one(
            &format!(
                "SELECT start_time_ns, end_time_ns FROM {q}.session_summary WHERE session_id = 'legacy-ns'"
            ),
            &[],
        )
        .await
        .expect("read migrated summary");
    assert_eq!(row.get::<_, i64>(0), 1_704_067_200_123_456_000);
    assert_eq!(row.get::<_, i64>(1), 1_704_067_200_654_321_000);

    let dirty = client
        .query_one(
            &format!(
                "SELECT min_ts_ns, max_ts_ns FROM {q}.session_summary_dirty WHERE session_id = 'legacy-ns'"
            ),
            &[],
        )
        .await
        .expect("legacy summary queued for precise recompute");
    assert_eq!(dirty.get::<_, i64>(0), 1_704_067_200_123_455_000);
    assert_eq!(dirty.get::<_, i64>(1), 1_704_067_200_654_322_000);

    let pending = client
        .query_one(
            &format!(
                "SELECT min_ts_ns, max_ts_ns FROM {q}.session_summary_dirty WHERE session_id = 'pending-only'"
            ),
            &[],
        )
        .await
        .expect("read migrated pending claim");
    assert_eq!(pending.get::<_, i64>(0), 1_704_153_600_123_455_000);
    assert_eq!(pending.get::<_, i64>(1), 1_704_153_600_654_322_000);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_ddl_supports_apostrophe_schema() {
    let schema = "thelake_ss_o'quote";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let client = pool.get().await.expect("client");
    let table_count: i64 = client
        .query_one(
            "SELECT count(*)::bigint FROM information_schema.tables WHERE table_schema = $1 AND table_name IN ('session_summary', 'session_summary_dirty')",
            &[&schema],
        )
        .await
        .expect("quoted schema tables")
        .get(0);
    assert_eq!(table_count, 2);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_concurrent_trigger_ensure_is_idempotent() {
    let schema = "thelake_ss_concurrent_trigger";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    pool.get()
        .await
        .expect("client")
        .execute(
            &format!("DROP TRIGGER IF EXISTS session_summary_dirty_bump_generation ON {q}.session_summary_dirty"),
            &[],
        )
        .await
        .expect("drop trigger for race test");
    let trigger_ddl = crate::session_summary::ddl::session_summary_table_ddls(schema)
        .into_iter()
        .find(|ddl| ddl.starts_with("DO $$"))
        .expect("trigger DDL");
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let ensure_trigger =
        |pool: Pool, barrier: std::sync::Arc<tokio::sync::Barrier>, ddl: String| {
            tokio::spawn(async move {
                let client = pool.get().await.expect("client");
                barrier.wait().await;
                client.execute(&ddl, &[]).await
            })
        };
    let first = ensure_trigger(pool.clone(), barrier.clone(), trigger_ddl.clone());
    let second = ensure_trigger(pool, barrier, trigger_ddl);
    first
        .await
        .expect("first ensure task")
        .expect("first trigger ensure");
    second
        .await
        .expect("second ensure task")
        .expect("second trigger ensure");
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
                "SELECT session_id, min_ts_ns, max_ts_ns FROM {q}.session_summary_dirty ORDER BY session_id"
            ),
            &[],
        )
        .await
        .expect("select dirty");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "s1");
    assert_eq!(
        rows[0].get::<_, i64>(1),
        crate::session_summary::time::to_ns(Utc.timestamp_opt(1, 0).unwrap())
    );
    assert_eq!(
        rows[0].get::<_, i64>(2),
        crate::session_summary::time::to_ns(Utc.timestamp_opt(50, 0).unwrap())
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
                "SELECT session_id, min_ts_ns, max_ts_ns FROM {q}.session_summary_dirty ORDER BY session_id"
            ),
            &[],
        )
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(
        rows[0].get::<_, i64>(1),
        crate::session_summary::time::to_ns(Utc.timestamp_opt(5, 0).unwrap())
    );
    assert_eq!(
        rows[0].get::<_, i64>(2),
        crate::session_summary::time::to_ns(Utc.timestamp_opt(10, 0).unwrap())
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
    maybe_after_traces_commit(
        false,
        "t1",
        1,
        true,
        std::time::Duration::ZERO,
        &hints,
        Some(&dirty),
    )
    .await;
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

    maybe_after_traces_commit(
        true,
        "t1",
        1,
        true,
        std::time::Duration::from_millis(1),
        &hints,
        Some(&dirty),
    )
    .await;
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

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_claim_ack_snapshot_preserves_newer_dirty() {
    let schema = "thelake_ss_ack";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 10)]))
        .await
        .expect("dirty");
    let (claims, snapshot) = claim_dirty(&pool, schema, 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].session_id, "s1");

    // Concurrent touch after snapshot.
    tokio::time::sleep(Duration::from_millis(20)).await;
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 50)]))
        .await
        .expect("concurrent dirty");

    // Simulate a backwards database clock correction or an older app-clock
    // writer. The trigger still advances the row generation.
    let client = pool.get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    client
        .execute(
            &format!("UPDATE {q}.session_summary_dirty SET updated_at = '2000-01-01T00:00:00Z' WHERE session_id = 's1'"),
            &[],
        )
        .await
        .expect("simulate backward timestamp");
    let touched_at: chrono::DateTime<Utc> = client
        .query_one(
            &format!("SELECT updated_at FROM {q}.session_summary_dirty WHERE session_id = 's1'"),
            &[],
        )
        .await
        .expect("read touched timestamp")
        .get(0);
    assert!(
        touched_at < snapshot,
        "test update has a timestamp older than the claim snapshot"
    );

    let acked = ack_dirty(&pool, schema, &claims, &claims[0].claim_token)
        .await
        .expect("ack");
    assert_eq!(
        acked, 0,
        "new generation must survive ack despite old timestamp"
    );
    let depth = dirty_depth(&pool, schema).await.expect("depth");
    assert_eq!(depth, 1);
    let claim_holder: Option<String> = client
        .query_one(
            &format!("SELECT claim_holder FROM {q}.session_summary_dirty WHERE session_id = 's1'"),
            &[],
        )
        .await
        .expect("claim state")
        .get(0);
    assert!(
        claim_holder.is_none(),
        "newer dirty row must be released for another claim"
    );
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_upsert_summary_absolute_replace_all_fields() {
    let schema = "thelake_ss_upsert";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let row = SummaryRow {
        session_id: "s1".into(),
        start_time_ns: crate::session_summary::time::to_ns(Utc.timestamp_opt(100, 0).unwrap()),
        end_time_ns: Some(crate::session_summary::time::to_ns(
            Utc.timestamp_opt(200, 0).unwrap(),
        )),
        observation_count: 2,
        error_count: 1,
        input_tokens: Some(11),
        output_tokens: Some(22),
        total_tokens: Some(33),
        total_cost: Some(0.5),
        agent_name: Some("agent-a".into()),
        user_id: Some("u1".into()),
        model_name: Some("gpt".into()),
    };
    upsert_summary_rows(&pool, schema, std::slice::from_ref(&row))
        .await
        .expect("upsert1");
    let replaced = SummaryRow {
        observation_count: 5,
        error_count: 0,
        total_tokens: Some(55),
        total_cost: Some(1.5),
        ..row
    };
    upsert_summary_rows(&pool, schema, std::slice::from_ref(&replaced))
        .await
        .expect("upsert2");

    let client = pool.get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    let r = client
        .query_one(
            &format!(
                "SELECT observation_count, error_count, total_tokens, total_cost, \
                        agent_name, user_id, model_name, input_tokens, output_tokens, \
                        start_time_ns, end_time_ns \
                 FROM {q}.session_summary WHERE session_id = 's1'"
            ),
            &[],
        )
        .await
        .expect("select");
    assert_eq!(r.get::<_, i64>(0), 5);
    assert_eq!(r.get::<_, i64>(1), 0);
    assert_eq!(r.get::<_, Option<i64>>(2), Some(55));
    assert!((r.get::<_, Option<f64>>(3).unwrap() - 1.5).abs() < 1e-9);
    assert_eq!(r.get::<_, Option<String>>(4).as_deref(), Some("agent-a"));
    assert_eq!(r.get::<_, Option<String>>(5).as_deref(), Some("u1"));
    assert_eq!(r.get::<_, Option<String>>(6).as_deref(), Some("gpt"));
    assert_eq!(r.get::<_, Option<i64>>(7), Some(11));
    assert_eq!(r.get::<_, Option<i64>>(8), Some(22));
    assert_eq!(r.get::<_, i64>(9), 100_000_000_000);
    assert_eq!(r.get::<_, Option<i64>>(10), Some(200_000_000_000));
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_claim_empty_dirty_ok() {
    let schema = "thelake_ss_empty";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let (claims, _) = claim_dirty(&pool, schema, 10, Duration::from_secs(30))
        .await
        .expect("claim");
    assert!(claims.is_empty());
    assert_eq!(dirty_depth(&pool, schema).await.expect("depth"), 0);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_dirty_claimers_get_disjoint_rows_and_expired_claims_recover() {
    let schema = "thelake_ss_claim_race";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("a", 1), span_at("b", 2)]))
        .await
        .expect("seed");

    let (left_result, right_result) = tokio::join!(
        claim_dirty(&pool, schema, 1, Duration::from_secs(30)),
        claim_dirty(&pool, schema, 1, Duration::from_secs(30)),
    );
    let (left, _) = left_result.expect("first claim");
    let (right, _) = right_result.expect("second claim");
    assert_eq!(left.len(), 1);
    assert_eq!(right.len(), 1);
    assert_ne!(left[0].session_id, right[0].session_id);

    let (none, _) = claim_dirty(&pool, schema, 10, Duration::from_secs(1))
        .await
        .expect("no active claims");
    assert!(none.is_empty());
    let q = crate::runtime_engine::quote_pg_ident(schema);
    pool.get()
        .await
        .expect("client")
        .execute(
            &format!(
                "UPDATE {q}.session_summary_dirty SET claim_until = now() - INTERVAL '1 second'"
            ),
            &[],
        )
        .await
        .expect("expire claims");
    let (reclaimed, _) = claim_dirty(&pool, schema, 10, Duration::from_secs(30))
        .await
        .expect("reclaim");
    assert_eq!(reclaimed.len(), 2);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_dirty_ack_requires_current_claim_token() {
    let schema = "thelake_ss_claim_token";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("a", 1)]))
        .await
        .expect("seed");
    let (old, _snapshot) = claim_dirty(&pool, schema, 1, Duration::from_secs(30))
        .await
        .expect("claim");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    pool.get().await.expect("client").execute(
        &format!("UPDATE {q}.session_summary_dirty SET claim_holder = 'new-token', claim_until = now() + INTERVAL '30 seconds'"), &[]
    ).await.expect("steal token");
    let acked = ack_dirty(&pool, schema, &old, &old[0].claim_token)
        .await
        .expect("stale ack");
    assert_eq!(acked, 0);
    assert_eq!(dirty_depth(&pool, schema).await.expect("row remains"), 1);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_expired_claim_cannot_publish_summary() {
    let schema = "thelake_ss_claim_publish";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 1)]))
        .await
        .expect("seed");
    let (claims, _snapshot) = claim_dirty(&pool, schema, 1, Duration::from_secs(30))
        .await
        .expect("claim");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    pool.get()
        .await
        .expect("client")
        .execute(
            &format!("UPDATE {q}.session_summary_dirty SET claim_holder = 'new-owner', claim_until = now() + INTERVAL '30 seconds'"),
            &[],
        )
        .await
        .expect("reclaim");
    let summary = SummaryRow {
        session_id: "s1".into(),
        start_time_ns: crate::session_summary::time::to_ns(Utc.timestamp_opt(1, 0).unwrap()),
        end_time_ns: None,
        observation_count: 1,
        error_count: 0,
        input_tokens: None,
        output_tokens: None,
        total_tokens: None,
        total_cost: None,
        agent_name: None,
        user_id: None,
        model_name: None,
    };
    let result = publish_claimed_summary_rows(
        &pool,
        schema,
        None,
        &claims,
        &claims[0].claim_token,
        &[summary],
    )
    .await;
    assert!(result.is_err(), "stale claim cannot publish");
    let count: i64 = pool
        .get()
        .await
        .expect("client")
        .query_one(
            &format!("SELECT count(*)::bigint FROM {q}.session_summary"),
            &[],
        )
        .await
        .expect("summary count")
        .get(0);
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_dirty_generation_fences_stale_publication() {
    let schema = "thelake_ss_generation_fence";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let dirty = SessionSummaryDirty::new(pool.clone(), schema, "t1");
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 1)]))
        .await
        .expect("seed dirty row");
    let (claims, _) = claim_dirty(&pool, schema, 1, Duration::from_secs(30))
        .await
        .expect("claim dirty row");
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 2)]))
        .await
        .expect("touch claimed row");
    let summary = SummaryRow {
        session_id: "s1".into(),
        start_time_ns: crate::session_summary::time::to_ns(Utc.timestamp_opt(1, 0).unwrap()),
        end_time_ns: None,
        observation_count: 1,
        error_count: 0,
        input_tokens: None,
        output_tokens: None,
        total_tokens: None,
        total_cost: None,
        agent_name: None,
        user_id: None,
        model_name: None,
    };

    let result = publish_claimed_summary_rows(
        &pool,
        schema,
        None,
        &claims,
        &claims[0].claim_token,
        &[summary],
    )
    .await;
    assert!(
        result.is_err(),
        "changed generation must reject stale publication"
    );
    assert_eq!(dirty_depth(&pool, schema).await.expect("dirty depth"), 1);
    let q = crate::runtime_engine::quote_pg_ident(schema);
    let summaries: i64 = pool
        .get()
        .await
        .expect("client")
        .query_one(
            &format!("SELECT count(*)::bigint FROM {q}.session_summary"),
            &[],
        )
        .await
        .expect("summary count")
        .get(0);
    assert_eq!(summaries, 0, "stale summary must not be published");
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_legacy_dirty_update_without_generation_advances_fence() {
    let schema = "thelake_ss_legacy_generation";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    let client = pool.get().await.expect("client");
    // Simulate a mixed-version writer that knows the old schema and omits the
    // newly introduced generation column from its conflict update.
    client.execute(
        &format!("INSERT INTO {q}.session_summary_dirty (session_id, min_ts_ns, max_ts_ns, updated_at) VALUES ('legacy', 10000000000, 10000000000, now())"),
        &[],
    ).await.expect("legacy insert");
    let initial: i64 = client
        .query_one(
            &format!(
                "SELECT generation FROM {q}.session_summary_dirty WHERE session_id = 'legacy'"
            ),
            &[],
        )
        .await
        .expect("initial generation")
        .get(0);
    client.execute(
        &format!("INSERT INTO {q}.session_summary_dirty (session_id, min_ts_ns, max_ts_ns, updated_at) VALUES ('legacy', 20000000000, 20000000000, now()) ON CONFLICT (session_id) DO UPDATE SET min_ts_ns = LEAST({q}.session_summary_dirty.min_ts_ns, EXCLUDED.min_ts_ns), max_ts_ns = GREATEST({q}.session_summary_dirty.max_ts_ns, EXCLUDED.max_ts_ns), updated_at = EXCLUDED.updated_at"),
        &[],
    ).await.expect("legacy conflict update");
    let after: i64 = client
        .query_one(
            &format!(
                "SELECT generation FROM {q}.session_summary_dirty WHERE session_id = 'legacy'"
            ),
            &[],
        )
        .await
        .expect("updated generation")
        .get(0);
    assert_eq!(
        after,
        initial + 1,
        "legacy writes must advance the publication fence"
    );
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_shared_dirty_claims_are_workspace_scoped() {
    let schema = "thelake_ss_shared_claim";
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
    let pool = Pool::builder(mgr).max_size(4).build().expect("pool");
    let client = pool.get().await.expect("client");
    crate::session_summary::ensure_shared_session_summary_tables(&client, schema)
        .await
        .expect("shared DDL");
    let q = crate::runtime_engine::quote_pg_ident(schema);
    client
        .execute(
            &format!("TRUNCATE {q}.session_summary, {q}.session_summary_dirty"),
            &[],
        )
        .await
        .expect("truncate");
    drop(client);

    for tenant in ["a", "b"] {
        let dirty = SessionSummaryDirty::new_for_workspace(pool.clone(), schema, tenant);
        dirty
            .upsert_dirty(&fold_dirty_hints(&[span_at("same", 1)]))
            .await
            .expect("seed tenant");
    }
    let (claims_a, _snapshot_a) = crate::session_summary::reduce::claim_dirty_for_workspace(
        &pool,
        schema,
        "a",
        10,
        Duration::from_secs(30),
    )
    .await
    .expect("claim a");
    let (claims_b, _snapshot_b) = crate::session_summary::reduce::claim_dirty_for_workspace(
        &pool,
        schema,
        "b",
        10,
        Duration::from_secs(30),
    )
    .await
    .expect("claim b");
    assert_eq!(claims_a.len(), 1);
    assert_eq!(claims_b.len(), 1);
    assert_ne!(claims_a[0].claim_token, claims_b[0].claim_token);
    assert_eq!(
        publish_claimed_summary_rows(
            &pool,
            schema,
            Some("a"),
            &claims_a,
            &claims_a[0].claim_token,
            &[],
        )
        .await
        .expect("ack a")
        .0,
        1
    );
    assert_eq!(
        publish_claimed_summary_rows(
            &pool,
            schema,
            Some("b"),
            &claims_b,
            &claims_b[0].claim_token,
            &[],
        )
        .await
        .expect("ack b")
        .0,
        1
    );
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_ensure_product_hot_attrs_activates_when_missing() {
    use crate::config::Config;
    use crate::promotion::load_active_telemetry_columns_manifests;
    use crate::runtime_engine::DuckLakeScopeResolver;
    use crate::session_summary::ensure_product_hot_attrs_for_scope;
    use crate::sql::llm::llm_promo;
    use crate::storage::ducklake::PhysicalScope;
    use std::sync::Arc;
    use tempfile::TempDir;

    let suffix = uuid::Uuid::new_v4().to_string().replace('-', "_");
    let schema = format!("thelake_ss_hot_{suffix}");
    let temp = TempDir::new().expect("temp");
    let mut config = Config::default();
    config.shrink_pools_for_tests();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.metadata_schema = schema.clone();
    config.ducklake.data_path = temp.path().join("data").to_string_lossy().into();
    config.ingest.flush_interval_seconds = 2;
    let config = Arc::new(config);

    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("connect");
    let scope = PhysicalScope::new(
        config.ducklake.metadata_path.clone(),
        config.ducklake.data_path.clone(),
        config.ducklake.catalog_alias.clone(),
        schema.clone(),
    );

    // Connect already ensures when enabled; deactivate to prove ensure re-activates.
    let client = resolver.pool().get().await.expect("client");
    let q = crate::runtime_engine::quote_pg_ident(&schema);
    client
        .execute(
            &format!("UPDATE {q}.promotion_specs SET status = 'inactive' WHERE status = 'active'"),
            &[],
        )
        .await
        .expect("deactivate");
    let before = load_active_telemetry_columns_manifests(&client, &schema)
        .await
        .expect("load before");
    assert!(
        before.is_empty(),
        "expected no active telemetry specs after deactivate"
    );
    drop(client);

    ensure_product_hot_attrs_for_scope(&resolver, &scope)
        .await
        .expect("ensure");
    let client = resolver.pool().get().await.expect("client");
    let after = load_active_telemetry_columns_manifests(&client, &schema)
        .await
        .expect("load after");
    let names: std::collections::HashSet<_> = after
        .iter()
        .flat_map(|m| m.columns.iter().map(|c| c.name.as_str()))
        .collect();
    for req in llm_promo().reduce_required_cols() {
        assert!(names.contains(req), "ensure missing {req}; have {names:?}");
    }

    // Idempotent.
    ensure_product_hot_attrs_for_scope(&resolver, &scope)
        .await
        .expect("ensure again");
}
