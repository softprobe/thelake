//! Postgres integration: every `sessions/search` filter against `session_summary`.
//! Run via `make test-lease-pg` / `make test-e2e` (`cargo test --lib postgres_ -- --ignored`).

use crate::api::llm::query::{SessionOrderBy, SessionSearchRequest, SortDirection};
use crate::api::sql_support::encode_cursor;
use crate::session_summary::ensure_session_summary_tables;
use crate::session_summary::list::search_session_summary;
use crate::session_summary::reduce::{upsert_summary_rows, SummaryRow};
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

fn ts(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn row(
    id: &str,
    start: i64,
    end: i64,
    errors: i64,
    agent: &str,
    user: &str,
    model: &str,
    tokens: i64,
    cost: f64,
) -> SummaryRow {
    SummaryRow {
        session_id: id.into(),
        start_time: ts(start),
        end_time: Some(ts(end)),
        observation_count: 3,
        error_count: errors,
        input_tokens: Some(tokens / 2),
        output_tokens: Some(tokens / 2),
        total_tokens: Some(tokens),
        total_cost: Some(cost),
        agent_name: Some(agent.into()),
        user_id: Some(user.into()),
        model_name: Some(model.into()),
    }
}

fn base_req(from: i64, to: i64) -> SessionSearchRequest {
    SessionSearchRequest {
        from: ts(from),
        to: ts(to),
        has_errors: None,
        user_id: None,
        model_name: None,
        agent_name: None,
        roots_only: true,
        order_by: SessionOrderBy::StartTime,
        order: SortDirection::Desc,
        limit: Some(50),
        cursor: None,
    }
}

async fn seed_filter_fixture(pool: &Pool, schema: &str) {
    // Window [1000, 2000]; outside row at 50 must never appear.
    upsert_summary_rows(
        pool,
        schema,
        &[
            row(
                "sess-ok", 1100, 1150, 0, "agent-a", "u1", "gpt-4o", 100, 0.1,
            ),
            row(
                "sess-err", 1200, 1400, 2, "agent-b", "u2", "claude", 200, 0.2,
            ),
            row(
                "sess-mix", 1400, 1500, 1, "agent-a", "u1", "claude", 50, 0.05,
            ),
            row("sess-old", 50, 60, 9, "agent-a", "u1", "gpt-4o", 999, 9.0),
        ],
    )
    .await
    .expect("seed");
}

fn ids(resp: &crate::api::llm::query::SessionSearchResponse) -> Vec<&str> {
    resp.items.iter().map(|s| s.session_id.as_str()).collect()
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_list_empty_table_returns_empty_page() {
    let schema = "thelake_ss_list_empty";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    let resp = search_session_summary(&pool, schema, &base_req(1000, 2000), 50)
        .await
        .expect("empty list");
    assert!(resp.items.is_empty());
    assert!(resp.next_cursor.is_none());
    assert!(resp.cursor_supported);
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_list_every_filter() {
    let schema = "thelake_ss_list_filters";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    seed_filter_fixture(&pool, schema).await;

    // --- time range ---
    let resp = search_session_summary(&pool, schema, &base_req(1000, 2000), 50)
        .await
        .expect("time");
    assert_eq!(ids(&resp), vec!["sess-mix", "sess-err", "sess-ok"]);
    assert!(resp.cursor_supported);
    assert!(resp.next_cursor.is_none());

    // --- has_errors ---
    let mut req = base_req(1000, 2000);
    req.has_errors = Some(true);
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("has_errors true");
    assert_eq!(ids(&resp), vec!["sess-mix", "sess-err"]);

    req.has_errors = Some(false);
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("has_errors false");
    assert_eq!(ids(&resp), vec!["sess-ok"]);

    // --- agent_name ---
    req = base_req(1000, 2000);
    req.agent_name = Some("agent-a".into());
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("agent");
    assert_eq!(ids(&resp), vec!["sess-mix", "sess-ok"]);

    // --- user_id ---
    req = base_req(1000, 2000);
    req.user_id = Some("u2".into());
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("user");
    assert_eq!(ids(&resp), vec!["sess-err"]);

    // --- model_name ---
    req = base_req(1000, 2000);
    req.model_name = Some("claude".into());
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("model");
    assert_eq!(ids(&resp), vec!["sess-mix", "sess-err"]);

    // --- combined filters ---
    req = base_req(1000, 2000);
    req.agent_name = Some("agent-a".into());
    req.user_id = Some("u1".into());
    req.model_name = Some("claude".into());
    req.has_errors = Some(true);
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("combined");
    assert_eq!(ids(&resp), vec!["sess-mix"]);

    // Field projection accuracy on the hit.
    let hit = &resp.items[0];
    assert_eq!(hit.observation_count, 3);
    assert_eq!(hit.error_count, 1);
    assert_eq!(hit.total_tokens, Some(50));
    assert!((hit.total_cost.unwrap() - 0.05).abs() < 1e-9);
    assert_eq!(hit.agent_name.as_deref(), Some("agent-a"));
    assert_eq!(hit.user_ids, vec!["u1".to_string()]);
    assert_eq!(hit.models, vec!["claude".to_string()]);
    assert_eq!(hit.trace_count, 0, "summary has no trace_count column");
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_list_cursor_and_orders() {
    let schema = "thelake_ss_list_cursor";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");
    seed_filter_fixture(&pool, schema).await;

    // Page size 1: three in-window rows → two follow-ups.
    let mut req = base_req(1000, 2000);
    req.limit = Some(1);
    let page1 = search_session_summary(&pool, schema, &req, 1)
        .await
        .expect("p1");
    assert_eq!(ids(&page1), vec!["sess-mix"]);
    let cursor = page1.next_cursor.expect("next");

    req.cursor = Some(cursor);
    let page2 = search_session_summary(&pool, schema, &req, 1)
        .await
        .expect("p2");
    assert_eq!(ids(&page2), vec!["sess-err"]);
    req.cursor = page2.next_cursor;
    let page3 = search_session_summary(&pool, schema, &req, 1)
        .await
        .expect("p3");
    assert_eq!(ids(&page3), vec!["sess-ok"]);
    assert!(page3.next_cursor.is_none());

    // Same-millisecond tiebreak on session_id (cursor keyset).
    // Costs/tokens sit between fixture extremes so later order_by asserts stay stable.
    upsert_summary_rows(
        &pool,
        schema,
        &[
            row("tie-b", 1600, 1610, 0, "a", "u", "m", 75, 0.075),
            row("tie-a", 1600, 1610, 0, "a", "u", "m", 75, 0.075),
        ],
    )
    .await
    .expect("tie seed");
    let mut req = base_req(1000, 2000);
    req.cursor = Some(encode_cursor(ts(1600), "tie-b"));
    let after = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("tie");
    assert!(
        after.items.iter().any(|s| s.session_id == "tie-a"),
        "session_id tiebreak must include tie-a after tie-b: {:?}",
        ids(&after)
    );
    assert!(
        !after.items.iter().any(|s| s.session_id == "tie-b"),
        "tie-b itself must be excluded by cursor"
    );

    // Re-seed clean fixture for order_by asserts (ties must not steal first place).
    let q = crate::runtime_engine::quote_pg_ident(schema);
    pool.get()
        .await
        .expect("client")
        .execute(
            &format!("TRUNCATE {q}.session_summary, {q}.session_summary_dirty"),
            &[],
        )
        .await
        .expect("truncate");
    seed_filter_fixture(&pool, schema).await;

    // order_by=error_count desc
    let mut req = base_req(1000, 2000);
    req.order_by = SessionOrderBy::ErrorCount;
    req.order = SortDirection::Desc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("order errors");
    assert!(!resp.cursor_supported);
    assert_eq!(resp.items[0].session_id, "sess-err");
    assert_eq!(resp.items[0].error_count, 2);

    // order_by=total_cost asc
    req.order_by = SessionOrderBy::TotalCost;
    req.order = SortDirection::Asc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("order cost");
    assert_eq!(resp.items[0].session_id, "sess-mix");

    // order_by=total_tokens desc
    req.order_by = SessionOrderBy::TotalTokens;
    req.order = SortDirection::Desc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("order tokens");
    assert_eq!(resp.items[0].session_id, "sess-err");

    // order_by=duration desc (sess-err is longest: 100s)
    req.order_by = SessionOrderBy::Duration;
    req.order = SortDirection::Desc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("order duration");
    assert_eq!(resp.items[0].session_id, "sess-err");

    // Bad cursor order → BadRequest
    let mut bad = base_req(1000, 2000);
    bad.order_by = SessionOrderBy::ErrorCount;
    bad.cursor = Some(encode_cursor(ts(1200), "x"));
    let err = search_session_summary(&pool, schema, &bad, 10)
        .await
        .expect_err("must reject");
    assert!(matches!(
        err,
        crate::session_summary::SessionSummaryListError::BadRequest(_)
    ));
}

#[tokio::test]
#[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
async fn postgres_session_summary_list_corner_cases() {
    let schema = "thelake_ss_list_corners";
    let pool = try_pg_pool(schema)
        .await
        .expect("ducklake-postgres required (make setup)");

    // Inclusive boundaries: start_time == from and == to must appear.
    upsert_summary_rows(
        &pool,
        schema,
        &[
            row("bound-lo", 1000, 1010, 0, "a", "u", "m", 1, 0.0),
            row("bound-hi", 2000, 2010, 0, "a", "u", "m", 1, 0.0),
            row("bound-mid", 1500, 1510, 0, "a", "u", "m", 1, 0.0),
            row("bound-out", 999, 1005, 0, "a", "u", "m", 1, 0.0),
            row("bound-out2", 2001, 2010, 0, "a", "u", "m", 1, 0.0),
        ],
    )
    .await
    .expect("bound seed");
    let resp = search_session_summary(&pool, schema, &base_req(1000, 2000), 50)
        .await
        .expect("bounds");
    assert_eq!(ids(&resp), vec!["bound-hi", "bound-mid", "bound-lo"]);

    // Quote escaping + trim on agent filter.
    upsert_summary_rows(
        &pool,
        schema,
        &[row(
            "quote-sess",
            1600,
            1610,
            0,
            "O'Brien",
            "u'1",
            "gpt-4o",
            10,
            0.01,
        )],
    )
    .await
    .expect("quote seed");
    let mut req = base_req(1000, 2000);
    req.agent_name = Some("  O'Brien  ".into());
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("quoted agent");
    assert_eq!(ids(&resp), vec!["quote-sess"]);
    req = base_req(1000, 2000);
    req.user_id = Some("u'1".into());
    assert_eq!(
        ids(&search_session_summary(&pool, schema, &req, 50)
            .await
            .expect("quoted user")),
        vec!["quote-sess"]
    );

    // Whitespace-only filters must be no-ops (not match empty agent_name).
    req = base_req(1000, 2000);
    req.agent_name = Some("   ".into());
    req.user_id = Some("\t".into());
    req.model_name = Some("".into());
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("whitespace noop");
    assert!(
        resp.items.len() >= 4,
        "whitespace filters must not narrow: {:?}",
        ids(&resp)
    );

    // NULL-ish fields: tokens/cost NULL → NULLS LAST; end_time NULL → duration 0;
    // empty agent/user/model project as empty arrays / null agent.
    upsert_summary_rows(
        &pool,
        schema,
        &[
            SummaryRow {
                session_id: "null-tokens".into(),
                start_time: ts(1700),
                end_time: None,
                observation_count: 1,
                error_count: 0,
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                total_cost: None,
                agent_name: None,
                user_id: None,
                model_name: None,
            },
            row("has-tokens", 1710, 1720, 0, "a", "u", "m", 999, 9.0),
        ],
    )
    .await
    .expect("null seed");

    let mut req = base_req(1000, 2000);
    req.order_by = SessionOrderBy::TotalTokens;
    req.order = SortDirection::Desc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("null tokens order");
    assert_eq!(resp.items[0].session_id, "has-tokens");
    assert!(
        resp.items
            .iter()
            .position(|s| s.session_id == "null-tokens")
            .unwrap()
            > resp
                .items
                .iter()
                .position(|s| s.session_id == "has-tokens")
                .unwrap(),
        "NULL tokens must sort after non-null in DESC: {:?}",
        ids(&resp)
    );

    req.order_by = SessionOrderBy::TotalCost;
    req.order = SortDirection::Desc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("null cost order");
    assert_eq!(resp.items[0].session_id, "has-tokens");

    req.order_by = SessionOrderBy::Duration;
    req.order = SortDirection::Desc;
    let resp = search_session_summary(&pool, schema, &req, 50)
        .await
        .expect("null end duration");
    assert!(
        resp.items
            .iter()
            .find(|s| s.session_id == "null-tokens")
            .is_some(),
        "null end_time row must still list"
    );
    // Duration DESC: has-tokens (10s) before null-tokens (0s via COALESCE).
    let pos_has = resp
        .items
        .iter()
        .position(|s| s.session_id == "has-tokens")
        .unwrap();
    let pos_null = resp
        .items
        .iter()
        .position(|s| s.session_id == "null-tokens")
        .unwrap();
    assert!(
        pos_has < pos_null,
        "zero-duration NULL end_time must sort after 10s: {:?}",
        ids(&resp)
    );

    let null_item = resp
        .items
        .iter()
        .find(|s| s.session_id == "null-tokens")
        .unwrap();
    assert!(null_item.agent_name.is_none());
    assert!(null_item.user_ids.is_empty());
    assert!(null_item.models.is_empty());
    assert!(null_item.total_tokens.is_none());
    assert!(null_item.end_time.is_none());

    // BadRequest corners
    let mut bad = base_req(2000, 1000);
    let err = search_session_summary(&pool, schema, &bad, 10)
        .await
        .expect_err("from>to");
    assert!(matches!(
        err,
        crate::session_summary::SessionSummaryListError::BadRequest(_)
    ));

    bad = base_req(1000, 2000);
    bad.cursor = Some("!!!not-base64!!!".into());
    let err = search_session_summary(&pool, schema, &bad, 10)
        .await
        .expect_err("bad cursor");
    assert!(matches!(
        err,
        crate::session_summary::SessionSummaryListError::BadRequest(_)
    ));

    bad.cursor = Some(encode_cursor(ts(1500), "x"));
    bad.order = SortDirection::Asc;
    let err = search_session_summary(&pool, schema, &bad, 10)
        .await
        .expect_err("cursor+asc");
    assert!(matches!(
        err,
        crate::session_summary::SessionSummaryListError::BadRequest(_)
    ));
}
