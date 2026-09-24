//! Postgres integration tests for session_summary DDL + dirty UPSERT.
//! Run via `make test-lease-pg` / `make test-e2e` (`cargo test --lib postgres_ -- --ignored`).

use super::*;
use crate::ingest_engine::maybe_after_traces_commit;
use crate::session_summary::reduce::{
    ack_dirty, claim_dirty, dirty_depth, upsert_summary_rows, SummaryRow,
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
    let (claims, snapshot) = claim_dirty(&pool, schema, 10).await.expect("claim");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].session_id, "s1");

    // Concurrent touch after snapshot.
    tokio::time::sleep(Duration::from_millis(20)).await;
    dirty
        .upsert_dirty(&fold_dirty_hints(&[span_at("s1", 50)]))
        .await
        .expect("concurrent dirty");

    let acked = ack_dirty(&pool, schema, &["s1".into()], snapshot)
        .await
        .expect("ack");
    assert_eq!(acked, 0, "newer updated_at must survive ack");
    let depth = dirty_depth(&pool, schema).await.expect("depth");
    assert_eq!(depth, 1);
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
        start_time: Utc.timestamp_opt(100, 0).unwrap(),
        end_time: Some(Utc.timestamp_opt(200, 0).unwrap()),
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
                        start_time, end_time \
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
    assert_eq!(
        r.get::<_, chrono::DateTime<Utc>>(9),
        Utc.timestamp_opt(100, 0).unwrap()
    );
    assert_eq!(
        r.get::<_, Option<chrono::DateTime<Utc>>>(10),
        Some(Utc.timestamp_opt(200, 0).unwrap())
    );
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_claim_empty_dirty_ok() {
    let schema = "thelake_ss_empty";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let (claims, _) = claim_dirty(&pool, schema, 10).await.expect("claim");
    assert!(claims.is_empty());
    assert_eq!(dirty_depth(&pool, schema).await.expect("depth"), 0);
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
