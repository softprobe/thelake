//! Field-accuracy matrix for promoted-only reduce SQL (in-memory DuckDB oracle).
//!
//! Covers every `session_summary` aggregate field without MAP/`attributes`.
//! Every case uses [`assert_full_fields`] — spot-checks alone are not enough.

use crate::sql::session_summary::compile_session_summary_reduce_sql;
use chrono::{TimeZone, Utc};
use duckdb::Connection;

fn setup_traces(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE traces (
           session_id VARCHAR,
           span_id VARCHAR,
           timestamp TIMESTAMP,
           end_timestamp TIMESTAMP,
           status_code VARCHAR,
           message_type VARCHAR,
           agent_name VARCHAR,
           observation_type VARCHAR,
           input_tokens BIGINT,
           output_tokens BIGINT,
           total_tokens BIGINT,
           total_cost DOUBLE,
           user_id VARCHAR,
           model_name VARCHAR,
           record_date DATE
         );",
    )
    .expect("create traces");
}

#[allow(clippy::too_many_arguments)]
fn insert(
    conn: &Connection,
    session_id: &str,
    span_id: &str,
    ts_secs: i64,
    end_secs: Option<i64>,
    status: &str,
    observation_type: &str,
    message_type: &str,
    agent: Option<&str>,
    in_tok: Option<i64>,
    out_tok: Option<i64>,
    tot_tok: Option<i64>,
    cost: Option<f64>,
    user_id: Option<&str>,
    model: Option<&str>,
) {
    let ts = format!("epoch_ms({}000)", ts_secs);
    let end = end_secs
        .map(|s| format!("epoch_ms({}000)", s))
        .unwrap_or_else(|| "NULL".into());
    let agent = agent
        .map(|a| format!("'{a}'"))
        .unwrap_or_else(|| "NULL".into());
    let in_tok = in_tok
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".into());
    let out_tok = out_tok
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".into());
    let tot_tok = tot_tok
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".into());
    let cost = cost.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
    let user_id = user_id
        .map(|u| format!("'{u}'"))
        .unwrap_or_else(|| "NULL".into());
    let model = model
        .map(|m| format!("'{m}'"))
        .unwrap_or_else(|| "NULL".into());
    let day = Utc
        .timestamp_opt(ts_secs, 0)
        .unwrap()
        .date_naive()
        .format("%Y-%m-%d");
    conn.execute_batch(&format!(
        "INSERT INTO traces VALUES (
           '{session_id}', '{span_id}', {ts}, {end}, '{status}', '{message_type}',
           {agent}, '{observation_type}', {in_tok}, {out_tok}, {tot_tok}, {cost},
           {user_id}, {model}, DATE '{day}'
         );"
    ))
    .expect("insert");
}

#[derive(Debug)]
struct Agg {
    session_id: String,
    start_us: i64,
    end_us: i64,
    observation_count: i64,
    error_count: i64,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    total_tokens: Option<i64>,
    total_cost: Option<f64>,
    agent_name: Option<String>,
    user_id: Option<String>,
    model_name: Option<String>,
}

fn run_reduce(conn: &Connection, session_ids: &[&str], from_secs: i64, to_secs: i64) -> Vec<Agg> {
    let ids: Vec<String> = session_ids.iter().map(|s| (*s).to_string()).collect();
    let from = Utc.timestamp_opt(from_secs, 0).unwrap();
    let to = Utc.timestamp_opt(to_secs, 0).unwrap();
    let sql = compile_session_summary_reduce_sql("traces", &ids, from, to).expect("sql");
    assert!(
        !sql.to_lowercase().contains("attributes"),
        "reduce SQL must not touch attributes"
    );
    let mut stmt = conn.prepare(&sql).expect("prepare");
    stmt.query_map([], |row| {
        Ok(Agg {
            session_id: row.get(0)?,
            start_us: row.get(1)?,
            end_us: row.get(2)?,
            observation_count: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
            error_count: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
            input_tokens: row.get(5)?,
            output_tokens: row.get(6)?,
            total_tokens: row.get(7)?,
            total_cost: row.get(8)?,
            agent_name: row.get(9)?,
            user_id: row.get(10)?,
            model_name: row.get(11)?,
        })
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

fn assert_full_fields(a: &Agg, expect: &Agg) {
    assert_eq!(a.session_id, expect.session_id, "session_id");
    assert_eq!(a.start_us, expect.start_us, "start_time");
    assert_eq!(a.end_us, expect.end_us, "end_time");
    assert_eq!(
        a.observation_count, expect.observation_count,
        "observation_count"
    );
    assert_eq!(a.error_count, expect.error_count, "error_count");
    assert_eq!(a.input_tokens, expect.input_tokens, "input_tokens");
    assert_eq!(a.output_tokens, expect.output_tokens, "output_tokens");
    assert_eq!(a.total_tokens, expect.total_tokens, "total_tokens");
    assert_eq!(a.total_cost, expect.total_cost, "total_cost");
    assert_eq!(a.agent_name, expect.agent_name, "agent_name");
    assert_eq!(a.user_id, expect.user_id, "user_id");
    assert_eq!(a.model_name, expect.model_name, "model_name");
}

fn by_id<'a>(rows: &'a [Agg], id: &str) -> &'a Agg {
    rows.iter()
        .find(|r| r.session_id == id)
        .unwrap_or_else(|| panic!("missing session {id}"))
}

#[test]
fn happy_multi_span_session_all_fields() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "s1",
        "a",
        100,
        Some(110),
        "OK",
        "generation",
        "msg",
        Some("agent-x"),
        Some(10),
        Some(20),
        Some(30),
        Some(0.5),
        Some("u1"),
        Some("gpt"),
    );
    insert(
        &conn,
        "s1",
        "b",
        200,
        Some(250),
        "OK",
        "generation",
        "msg",
        Some("agent-x"),
        Some(1),
        Some(2),
        Some(3),
        Some(0.1),
        Some("u1"),
        Some("gpt"),
    );
    let rows = run_reduce(&conn, &["s1"], 0, 1000);
    assert_eq!(rows.len(), 1);
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 100_000_000,
            end_us: 250_000_000,
            observation_count: 2,
            error_count: 0,
            input_tokens: Some(11),
            output_tokens: Some(22),
            total_tokens: Some(33),
            total_cost: Some(0.6),
            agent_name: Some("agent-x".into()),
            user_id: Some("u1".into()),
            model_name: Some("gpt".into()),
        },
    );
}

#[test]
fn error_count_and_zero_errors() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "err",
        "a",
        100,
        None,
        "ERROR",
        "generation",
        "msg",
        Some("e"),
        Some(1),
        Some(0),
        Some(1),
        Some(0.0),
        Some("ue"),
        Some("me"),
    );
    insert(
        &conn,
        "err",
        "b",
        101,
        None,
        "OK",
        "generation",
        "msg",
        Some("e"),
        Some(1),
        Some(0),
        Some(1),
        Some(0.0),
        Some("ue"),
        Some("me"),
    );
    insert(
        &conn,
        "ok",
        "c",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("o"),
        Some(1),
        Some(0),
        Some(1),
        Some(0.0),
        Some("uo"),
        Some("mo"),
    );
    let rows = run_reduce(&conn, &["err", "ok"], 0, 1000);
    assert_full_fields(
        by_id(&rows, "err"),
        &Agg {
            session_id: "err".into(),
            start_us: 100_000_000,
            end_us: 101_000_000,
            observation_count: 2,
            error_count: 1,
            input_tokens: Some(2),
            output_tokens: Some(0),
            total_tokens: Some(2),
            total_cost: Some(0.0),
            agent_name: Some("e".into()),
            user_id: Some("ue".into()),
            model_name: Some("me".into()),
        },
    );
    assert_full_fields(
        by_id(&rows, "ok"),
        &Agg {
            session_id: "ok".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(1),
            output_tokens: Some(0),
            total_tokens: Some(1),
            total_cost: Some(0.0),
            agent_name: Some("o".into()),
            user_id: Some("uo".into()),
            model_name: Some("mo".into()),
        },
    );
}

#[test]
fn recording_excluded_from_counts_and_tokens() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "s1",
        "a",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(5),
        Some(5),
        Some(10),
        Some(1.0),
        Some("u"),
        Some("m"),
    );
    insert(
        &conn,
        "s1",
        "rec",
        101,
        None,
        "OK",
        "recording",
        "msg",
        Some("ag"),
        Some(100),
        Some(100),
        Some(200),
        Some(9.0),
        Some("u"),
        Some("m"),
    );
    let rows = run_reduce(&conn, &["s1"], 0, 1000);
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(5),
            output_tokens: Some(5),
            total_tokens: Some(10),
            total_cost: Some(1.0),
            agent_name: Some("ag".into()),
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
}

#[test]
fn duplicate_span_id_uses_count_distinct() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "s1",
        "dup",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(1),
        Some(1),
        Some(2),
        Some(0.1),
        Some("u"),
        Some("m"),
    );
    insert(
        &conn,
        "s1",
        "dup",
        101,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(1),
        Some(1),
        Some(2),
        Some(0.1),
        Some("u"),
        Some("m"),
    );
    let rows = run_reduce(&conn, &["s1"], 0, 1000);
    // COUNT(DISTINCT span_id)=1 but SUM still includes both rows.
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 100_000_000,
            end_us: 101_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(2),
            output_tokens: Some(2),
            total_tokens: Some(4),
            total_cost: Some(0.2),
            agent_name: Some("ag".into()),
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
}

#[test]
fn null_typed_tokens_sum_null() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "s1",
        "a",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        None,
        None,
        None,
        None,
        Some("u"),
        Some("m"),
    );
    insert(
        &conn,
        "s1",
        "b",
        101,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        None,
        None,
        None,
        None,
        Some("u"),
        Some("m"),
    );
    let rows = run_reduce(&conn, &["s1"], 0, 1000);
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 100_000_000,
            end_us: 101_000_000,
            observation_count: 2,
            error_count: 0,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            total_cost: None,
            agent_name: Some("ag".into()),
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
}

#[test]
fn agent_from_typed_column_and_message_type_fallback() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "stamped",
        "a",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("from-auth"),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    insert(
        &conn, "fallback", "b", 100, None, "OK", "agent", "AgentBot", None, None, None, None, None,
        None, None,
    );
    let rows = run_reduce(&conn, &["stamped", "fallback"], 0, 1000);
    assert_full_fields(
        by_id(&rows, "stamped"),
        &Agg {
            session_id: "stamped".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            total_cost: None,
            agent_name: Some("from-auth".into()),
            user_id: None,
            model_name: None,
        },
    );
    assert_full_fields(
        by_id(&rows, "fallback"),
        &Agg {
            session_id: "fallback".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            total_cost: None,
            agent_name: Some("AgentBot".into()),
            user_id: None,
            model_name: None,
        },
    );
}

#[test]
fn multi_session_batch_independent() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "a",
        "1",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("aa"),
        Some(1),
        Some(0),
        Some(1),
        Some(0.1),
        Some("ua"),
        Some("m-a"),
    );
    insert(
        &conn,
        "b",
        "2",
        100,
        None,
        "ERROR",
        "generation",
        "msg",
        Some("bb"),
        Some(9),
        Some(1),
        Some(10),
        Some(2.0),
        Some("ub"),
        Some("m-b"),
    );
    let rows = run_reduce(&conn, &["a", "b"], 0, 1000);
    assert_eq!(rows.len(), 2);
    assert_full_fields(
        by_id(&rows, "a"),
        &Agg {
            session_id: "a".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(1),
            output_tokens: Some(0),
            total_tokens: Some(1),
            total_cost: Some(0.1),
            agent_name: Some("aa".into()),
            user_id: Some("ua".into()),
            model_name: Some("m-a".into()),
        },
    );
    assert_full_fields(
        by_id(&rows, "b"),
        &Agg {
            session_id: "b".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 1,
            input_tokens: Some(9),
            output_tokens: Some(1),
            total_tokens: Some(10),
            total_cost: Some(2.0),
            agent_name: Some("bb".into()),
            user_id: Some("ub".into()),
            model_name: Some("m-b".into()),
        },
    );
}

#[test]
fn absolute_replace_semantics_via_reaggregate() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "s1",
        "a",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(50),
        Some(0),
        Some(50),
        Some(1.0),
        Some("u"),
        Some("m"),
    );
    assert_full_fields(
        &run_reduce(&conn, &["s1"], 0, 1000)[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(50),
            output_tokens: Some(0),
            total_tokens: Some(50),
            total_cost: Some(1.0),
            agent_name: Some("ag".into()),
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
    insert(
        &conn,
        "s1",
        "b",
        200,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(5),
        Some(0),
        Some(5),
        Some(0.5),
        Some("u"),
        Some("m"),
    );
    assert_full_fields(
        &run_reduce(&conn, &["s1"], 0, 1000)[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 100_000_000,
            end_us: 200_000_000,
            observation_count: 2,
            error_count: 0,
            input_tokens: Some(55),
            output_tokens: Some(0),
            total_tokens: Some(55),
            total_cost: Some(1.5),
            agent_name: Some("ag".into()),
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
}

#[test]
fn clamp_window_excludes_early_history() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "s1",
        "old",
        100,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(1000),
        Some(0),
        Some(1000),
        Some(10.0),
        Some("u"),
        Some("m"),
    );
    insert(
        &conn,
        "s1",
        "new",
        950,
        None,
        "OK",
        "generation",
        "msg",
        Some("ag"),
        Some(5),
        Some(0),
        Some(5),
        Some(0.5),
        Some("u"),
        Some("m"),
    );
    let rows = run_reduce(&conn, &["s1"], 900, 1000);
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "s1".into(),
            start_us: 950_000_000,
            end_us: 950_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(5),
            output_tokens: Some(0),
            total_tokens: Some(5),
            total_cost: Some(0.5),
            agent_name: Some("ag".into()),
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
}

/// Stage 5: MAP bag values must not leak into reduce when typed cols are NULL.
#[test]
fn map_only_attrs_do_not_fill_typed_aggregates() {
    use crate::models::attr_keys::{gen_ai, sp};

    let conn = Connection::open_in_memory().unwrap();
    // Richer table than reduce reads — bag is deliberately populated.
    conn.execute_batch(
        "CREATE TABLE traces (
           session_id VARCHAR,
           span_id VARCHAR,
           timestamp TIMESTAMP,
           end_timestamp TIMESTAMP,
           status_code VARCHAR,
           message_type VARCHAR,
           agent_name VARCHAR,
           observation_type VARCHAR,
           input_tokens BIGINT,
           output_tokens BIGINT,
           total_tokens BIGINT,
           total_cost DOUBLE,
           user_id VARCHAR,
           model_name VARCHAR,
           record_date DATE,
           attributes MAP(VARCHAR, VARCHAR)
         );",
    )
    .expect("create");
    let day = Utc
        .timestamp_opt(100, 0)
        .unwrap()
        .date_naive()
        .format("%Y-%m-%d");
    let bag = format!(
        "MAP {{'{agent}': 'bag-agent', '{user}': 'bag-user', '{model}': 'bag-model', \
         '{in_tok}': '99', '{out_tok}': '88', '{tot}': '187', '{cost}': '9.9'}}",
        agent = sp::AGENT_NAME,
        user = sp::USER_ID,
        model = gen_ai::REQUEST_MODEL,
        in_tok = gen_ai::USAGE_INPUT_TOKENS,
        out_tok = gen_ai::USAGE_OUTPUT_TOKENS,
        tot = gen_ai::USAGE_TOTAL_TOKENS,
        cost = sp::COST_TOTAL,
    );
    conn.execute_batch(&format!(
        "INSERT INTO traces VALUES (
           'bag-only', 'a', epoch_ms(100000), NULL, 'OK', 'IgnoredName',
           NULL, 'generation', NULL, NULL, NULL, NULL, NULL, NULL,
           DATE '{day}', {bag}
         );"
    ))
    .expect("insert bag row");

    let rows = run_reduce(&conn, &["bag-only"], 0, 1000);
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "bag-only".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
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
    );
}

/// Stage 5: without auth agent_name or agent observation, generation message_type is ignored.
#[test]
fn generation_message_type_without_auth_agent_stays_null() {
    let conn = Connection::open_in_memory().unwrap();
    setup_traces(&conn);
    insert(
        &conn,
        "no-agent",
        "a",
        100,
        None,
        "OK",
        "generation",
        "WouldBeWrongIfUsed",
        None,
        Some(1),
        Some(1),
        Some(2),
        Some(0.01),
        Some("u"),
        Some("m"),
    );
    let rows = run_reduce(&conn, &["no-agent"], 0, 1000);
    assert_full_fields(
        &rows[0],
        &Agg {
            session_id: "no-agent".into(),
            start_us: 100_000_000,
            end_us: 100_000_000,
            observation_count: 1,
            error_count: 0,
            input_tokens: Some(1),
            output_tokens: Some(1),
            total_tokens: Some(2),
            total_cost: Some(0.01),
            agent_name: None,
            user_id: Some("u".into()),
            model_name: Some("m".into()),
        },
    );
}
