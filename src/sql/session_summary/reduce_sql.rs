//! Promoted-only DuckLake aggregate SQL for `session_summary` reduce **and** rebuild.
//!
//! **Hard rule:** never reference `attributes` / MAP bags. List path may use
//! `prefer_attr_*`; reduce/rebuild must not.

use crate::sql::literal::sql_string_literal;
use crate::sql::llm::llm_promo;
use crate::sql::QueryWindow;
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};

/// Columns written by absolute `session_summary` UPSERT.
///
/// Order is the bind-parameter order used by [`crate::session_summary::reduce::upsert_summary_rows`].
/// `session_id` is the conflict key (not updated); every other column is replaced from `EXCLUDED`.
const SESSION_SUMMARY_UPSERT_COLUMNS: &[&str] = &[
    "session_id",
    "start_time_ns",
    "end_time_ns",
    "observation_count",
    "error_count",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "total_cost",
    "agent_name",
    "user_id",
    "model_name",
    "updated_at",
];

/// Compile time-scoped `GROUP BY session_id` SQL over `from_table` (promoted cols only).
///
/// - `session_ids = Some([...])` — reduce path (dirty claim IN-list; must be non-empty).
/// - `session_ids = None` — rebuild path (window-wide; still `session_id <> ''`).
pub fn compile_session_summary_aggregate_sql(
    from_table: &str,
    session_ids: Option<&[String]>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    compile_session_summary_aggregate_sql_for_workspace(from_table, session_ids, None, from, to)
}

pub fn compile_session_summary_aggregate_sql_for_workspace(
    from_table: &str,
    session_ids: Option<&[String]>,
    workspace_id: Option<&str>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    let window = QueryWindow::try_new(from, to)
        .map_err(|e| anyhow::anyhow!("session_summary aggregate SQL: {e}"))?;
    if from_table.is_empty() || from_table.contains(';') {
        bail!("invalid from_table for aggregate SQL");
    }
    if let Some(ids) = session_ids {
        if ids.is_empty() {
            bail!("session_summary reduce SQL requires at least one session_id");
        }
    }

    let promo = llm_promo();

    let session_pred = match session_ids {
        Some(ids) => {
            let in_list = ids
                .iter()
                .map(|id| sql_string_literal(id))
                .collect::<Vec<_>>()
                .join(", ");
            format!("session_id IN ({in_list}) AND session_id <> ''")
        }
        None => "session_id <> ''".to_string(),
    };
    let ownership_pred = workspace_id
        .map(|id| format!("workspace_id = {}", sql_string_literal(id)))
        .unwrap_or_default();

    // Typed agent resolve (no attributes['sp.agent.name']).
    let agent_expr = format!(
        "COALESCE( \
           NULLIF(arg_min(agent_name, timestamp) FILTER (WHERE NULLIF(agent_name, '') IS NOT NULL), ''), \
           arg_min(message_type, timestamp) FILTER (WHERE COALESCE({obs}, '') = 'agent') \
         )",
        obs = promo.observation_type,
    );

    let observation_type = promo.observation_type;
    let input_tokens = promo.input_tokens;
    let output_tokens = promo.output_tokens;
    let total_tokens = promo.total_tokens;
    let total_cost = promo.total_cost;
    let user_id = promo.user_id;
    let model_name = promo.model_name;

    // Predicate order: identity → timestamp (one clock) → observation filter.
    let where_prefix = if ownership_pred.is_empty() {
        format!("{session_pred} AND ")
    } else {
        format!("{ownership_pred} AND {session_pred} AND ")
    };
    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            SESSION_SUMMARY_AGGREGATE_SQL
                .replace("{{from_table}}", from_table)
                .replace("{{where_prefix}}", &where_prefix)
                .replace("{{timestamp_filter}}", bound)
                .replace("{{observation_type}}", observation_type)
                .replace("{{input_tokens}}", input_tokens)
                .replace("{{output_tokens}}", output_tokens)
                .replace("{{total_tokens}}", total_tokens)
                .replace("{{total_cost}}", total_cost)
                .replace("{{user_id}}", user_id)
                .replace("{{model_name}}", model_name)
                .replace("{{agent_expr}}", &agent_expr)
        })
        .into_sql())
}

/// Reduce path: dirty session IN-list required.
pub fn compile_session_summary_reduce_sql(
    from_table: &str,
    session_ids: &[String],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    compile_session_summary_aggregate_sql_for_workspace(
        from_table,
        Some(session_ids),
        None,
        from,
        to,
    )
}

pub fn compile_session_summary_reduce_sql_for_workspace(
    from_table: &str,
    session_ids: &[String],
    workspace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    compile_session_summary_aggregate_sql_for_workspace(
        from_table,
        Some(session_ids),
        Some(workspace_id),
        from,
        to,
    )
}

/// Rebuild path: window-wide (no IN-list).
pub fn compile_session_summary_rebuild_sql(
    from_table: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    compile_session_summary_rebuild_sql_for_workspace(from_table, None, from, to)
}

pub fn compile_session_summary_rebuild_sql_for_workspace(
    from_table: &str,
    workspace_id: Option<&str>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    compile_session_summary_aggregate_sql_for_workspace(from_table, None, workspace_id, from, to)
}

const SESSION_SUMMARY_UPSERT_ARRAY_TYPES: &[&str] = &[
    "text[]",
    "bigint[]",
    "bigint[]",
    "bigint[]",
    "bigint[]",
    "bigint[]",
    "bigint[]",
    "bigint[]",
    "double precision[]",
    "text[]",
    "text[]",
    "text[]",
    "timestamptz[]",
];
const SESSION_SUMMARY_AGGREGATE_SQL: &str = include_str!("session_summary_aggregate.sql");
const SESSION_SUMMARY_UPSERT_SQL: &str = include_str!("summary_upsert.sql");

/// Absolute UPSERT into catalog Postgres `session_summary`.
///
/// Keep this SQL independent of the batch length. A VALUES list creates a new
/// prepared statement for every batch size, and PostgreSQL retains each plan on
/// the pooled connection. Parallel typed arrays keep both the SQL and cached
/// plan bounded.
pub fn compile_session_summary_upsert_sql(schema_quoted: &str) -> String {
    compile_session_summary_upsert_sql_for_scope(schema_quoted, false)
}

pub fn compile_session_summary_upsert_sql_for_workspace(schema_quoted: &str) -> String {
    compile_session_summary_upsert_sql_for_scope(schema_quoted, true)
}

fn compile_session_summary_upsert_sql_for_scope(schema_quoted: &str, workspace: bool) -> String {
    let mut columns =
        Vec::with_capacity(SESSION_SUMMARY_UPSERT_COLUMNS.len() + usize::from(workspace));
    if workspace {
        columns.push("workspace_id");
    }
    columns.extend_from_slice(SESSION_SUMMARY_UPSERT_COLUMNS);

    let array_placeholders = SESSION_SUMMARY_UPSERT_ARRAY_TYPES
        .iter()
        .enumerate()
        .map(|(index, ty)| format!("${}::{ty}", index + 1 + usize::from(workspace)))
        .collect::<Vec<_>>()
        .join(", ");
    let selected_columns = SESSION_SUMMARY_UPSERT_COLUMNS
        .iter()
        .map(|column| format!("input.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let select = if workspace {
        format!("$1::text, {selected_columns}")
    } else {
        selected_columns
    };
    let keys = if workspace {
        "workspace_id, session_id"
    } else {
        "session_id"
    };
    let set_list = columns
        .iter()
        .filter(|column| **column != "workspace_id" && **column != "session_id")
        .map(|column| format!("{column} = EXCLUDED.{column}"))
        .collect::<Vec<_>>()
        .join(",\n           ");

    SESSION_SUMMARY_UPSERT_SQL
        .replace("{{schema_quoted}}", schema_quoted)
        .replace("{{insert_columns}}", &columns.join(", "))
        .replace("{{select_columns}}", &select)
        .replace("{{array_placeholders}}", &array_placeholders)
        .replace(
            "{{input_columns}}",
            &SESSION_SUMMARY_UPSERT_COLUMNS.join(", "),
        )
        .replace("{{conflict_keys}}", keys)
        .replace("{{set_list}}", &set_list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_window() -> (DateTime<Utc>, DateTime<Utc>) {
        (
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap(),
        )
    }

    fn assert_aggregate_invariants(sql: &str) {
        use crate::models::attr_keys::{enduser, sp};
        use crate::sql::assert_sql_has_otlp_time_predicates;
        assert_sql_has_otlp_time_predicates(sql);
        assert!(sql.contains("COUNT(DISTINCT span_id)"));
        assert!(sql.contains("COALESCE(observation_type, '') <> 'recording'"));
        assert!(sql.contains("SUM(input_tokens)"));
        assert!(sql.contains("SUM(total_cost)"));
        let lower = sql.to_lowercase();
        assert!(!lower.contains("attributes"), "{sql}");
        assert!(!lower.contains("resource_attributes"), "{sql}");
        assert!(!sql.contains("SELECT *"));
        for banned in [sp::AGENT_NAME, sp::USER_ID, enduser::ID, sp::COST_TOTAL] {
            assert!(
                !sql.contains(banned),
                "aggregate SQL must not embed bag key {banned}: {sql}"
            );
        }
    }

    #[test]
    fn reduce_sql_has_pushdown_and_no_attributes() {
        let (from, to) = sample_window();
        let sql =
            compile_session_summary_reduce_sql("traces", &["s1".into(), "s2".into()], from, to)
                .expect("sql");
        assert_aggregate_invariants(&sql);
        assert!(sql.contains("session_id IN"));
        assert!(!sql.contains("record_date"));
        let sid = sql.find("session_id IN").unwrap();
        let ts = sql.find("timestamp >=").unwrap();
        assert!(sid < ts);
    }

    #[test]
    fn rebuild_sql_window_wide_no_in_list() {
        let (from, to) = sample_window();
        let sql = compile_session_summary_rebuild_sql("traces", from, to).expect("sql");
        assert_aggregate_invariants(&sql);
        assert!(!sql.contains("session_id IN"));
        assert!(sql.contains("session_id <> ''"));
    }

    #[test]
    fn reduce_sql_rejects_empty_ids() {
        let (from, to) = sample_window();
        assert!(compile_session_summary_reduce_sql("traces", &[], from, to).is_err());
    }

    #[test]
    fn aggregate_sql_rejects_inverted_range() {
        let (from, to) = sample_window();
        assert!(compile_session_summary_rebuild_sql("traces", to, from).is_err());
        assert!(compile_session_summary_reduce_sql("traces", &["s".into()], to, from).is_err());
    }

    #[test]
    fn upsert_sql_is_absolute_replace() {
        let sql = compile_session_summary_upsert_sql("\"meta\"");
        assert!(sql.contains("ON CONFLICT (session_id) DO UPDATE SET"));
        assert!(sql.contains("observation_count = EXCLUDED.observation_count"));
        assert!(
            sql.contains("$13::timestamptz[]"),
            "fixed array parameters missing in {sql}"
        );
        assert!(sql.contains(SESSION_SUMMARY_UPSERT_COLUMNS.join(", ").as_str()));
    }

    #[test]
    fn workspace_reduce_and_upsert_use_composite_ownership_keys() {
        let (from, to) = sample_window();
        let reduce = compile_session_summary_reduce_sql_for_workspace(
            "traces",
            &["s1".into()],
            "workspace'42",
            from,
            to,
        )
        .expect("workspace reduce SQL");
        assert!(reduce.contains("workspace_id = 'workspace''42'"));

        let upsert = compile_session_summary_upsert_sql_for_workspace("\"meta\"");
        assert!(upsert.contains("(workspace_id, session_id) DO UPDATE"));
        assert!(upsert.contains("INSERT INTO \"meta\".session_summary (workspace_id, session_id"));
        assert!(upsert.contains("FROM UNNEST($2::text[], $3::bigint[]"));
        assert!(!upsert.contains("VALUES ("));
    }

    #[test]
    fn summary_upsert_sql_is_constant_size_and_uses_typed_arrays() {
        let sql = compile_session_summary_upsert_sql("\"meta\"");
        assert!(sql.contains("FROM UNNEST($1::text[], $2::bigint[]"));
        assert!(sql.contains("$13::timestamptz[]"));
        assert!(!sql.contains("VALUES ("));
    }

    #[test]
    fn reduce_sql_deduplicates_usage_leaf_tokens_excludes_http_body_and_handles_cycles() {
        let conn = duckdb::Connection::open_in_memory().expect("duckdb");
        conn.execute_batch(include_str!("../schema/traces.sql"))
            .expect("create traces");
        conn.execute_batch(
            "INSERT INTO traces (
               session_id, trace_id, span_id, parent_span_id, app_id, message_type,
               timestamp, end_timestamp, status_code, observation_type,
               input_tokens, output_tokens, total_tokens, total_cost, user_id, model_name
             ) VALUES
             ('s_otel', 'tr_1', 'sp_agent', NULL, 'travel-agent', 'concierge',
              TIMESTAMP_NS '2024-01-01 10:00:00', TIMESTAMP_NS '2024-01-01 10:00:05', 'OK', 'agent',
              25854, 2447, 29000, 0.05, 'u_1', NULL),
             ('s_otel', 'tr_1', 'sp_gen', 'sp_agent', 'travel-agent', 'chat',
              TIMESTAMP_NS '2024-01-01 10:00:01', TIMESTAMP_NS '2024-01-01 10:00:04', 'OK', 'generation',
              25854, 2447, 29000, 0.05, NULL, 'gemini-2.5-flash'),
             ('s_otel', 'tr_1', 'sp_tool', 'sp_agent', 'travel-agent', 'execute_tool',
              TIMESTAMP_NS '2024-01-01 10:00:02', TIMESTAMP_NS '2024-01-01 10:00:03', 'OK', 'tool',
              NULL, NULL, NULL, NULL, NULL, NULL),
             ('s_otel', 'tr_1', 'sp_body', 'sp_tool', 'travel-agent', 'softprobe.http.body.capture',
              TIMESTAMP_NS '2024-01-01 10:00:03', TIMESTAMP_NS '2024-01-01 10:00:06', 'OK', 'http_body',
              NULL, NULL, NULL, NULL, NULL, NULL),
             ('s_cyc', 'tr_c', 'c1', 'c2', 'app', 'agent',
              TIMESTAMP_NS '2024-01-01 10:00:00', NULL, 'OK', 'agent',
              NULL, NULL, NULL, NULL, NULL, NULL),
             ('s_cyc', 'tr_c', 'c2', 'c1', 'app', 'chat',
              TIMESTAMP_NS '2024-01-01 10:00:01', NULL, 'OK', 'generation',
              40, 10, NULL, NULL, NULL, 'gpt-4o');",
        )
        .expect("insert");

        let (from, to) = sample_window();
        let sql = compile_session_summary_reduce_sql(
            "traces",
            &["s_otel".into(), "s_cyc".into()],
            from,
            to,
        )
        .expect("reduce sql");
        assert_aggregate_invariants(&sql);
        crate::sql::ensure_fact_scan_uses_timestamp_pruning(&conn, &sql)
            .expect("reduce pruning gate");

        #[derive(Debug, PartialEq, Eq)]
        struct ReducedSessionRow {
            session_id: String,
            observation_count: i64,
            input_tokens: Option<i64>,
            output_tokens: Option<i64>,
            total_tokens: Option<i64>,
        }

        let mut stmt = conn.prepare(&sql).expect("prepare");
        let mut rows: Vec<ReducedSessionRow> = stmt
            .query_map([], |row| {
                Ok(ReducedSessionRow {
                    session_id: row.get("session_id")?,
                    observation_count: row.get("observation_count")?,
                    input_tokens: row.get("input_tokens")?,
                    output_tokens: row.get("output_tokens")?,
                    total_tokens: row.get("total_tokens")?,
                })
            })
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        rows.sort_by(|a, b| a.session_id.cmp(&b.session_id));

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            ReducedSessionRow {
                session_id: "s_cyc".into(),
                observation_count: 2,
                input_tokens: Some(40),
                output_tokens: Some(10),
                total_tokens: Some(50),
            },
            "cycle terminates and computes missing total_tokens = input + output"
        );
        assert_eq!(
            rows[1],
            ReducedSessionRow {
                session_id: "s_otel".into(),
                observation_count: 3,
                input_tokens: Some(25854),
                output_tokens: Some(2447),
                total_tokens: Some(29000),
            },
            "usage-leaf deduplicates parent agent rollup, preserves reasoning total_tokens, and excludes http_body from observation_count"
        );
    }
}
