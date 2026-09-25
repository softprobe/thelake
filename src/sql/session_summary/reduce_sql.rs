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
    "start_time",
    "end_time",
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
        .map(|id| format!("tenant_id = {}", sql_string_literal(id)))
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
    Ok(window
        .bind_scan("", |bound| {
            let where_sql = if ownership_pred.is_empty() {
                format!(
                    "{session_pred} AND {bound} AND COALESCE({observation_type}, '') <> 'recording'"
                )
            } else {
                format!(
                    "{ownership_pred} AND {session_pred} AND {bound} AND COALESCE({observation_type}, '') <> 'recording'"
                )
            };
            format!(
                "SELECT \
                   session_id, \
                   CAST(epoch_us(MIN(timestamp)) AS BIGINT) AS start_time_us, \
                   CAST(epoch_us(MAX(COALESCE(end_timestamp, timestamp))) AS BIGINT) AS end_time_us, \
                   COUNT(DISTINCT span_id)::BIGINT AS observation_count, \
                   SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::BIGINT AS error_count, \
                   SUM({input_tokens})::BIGINT AS input_tokens, \
                   SUM({output_tokens})::BIGINT AS output_tokens, \
                   SUM({total_tokens})::BIGINT AS total_tokens, \
                   SUM({total_cost}) AS total_cost, \
                   {agent_expr} AS agent_name, \
                   arg_min({user_id}, timestamp) FILTER (WHERE NULLIF({user_id}, '') IS NOT NULL) AS user_id, \
                   arg_min({model_name}, timestamp) FILTER (WHERE NULLIF({model_name}, '') IS NOT NULL) AS model_name \
                 FROM {from_table} \
                 WHERE {where_sql} \
                 GROUP BY session_id"
            )
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

/// Build `($1, $2, …), ($n, …)` for a multi-row INSERT.
fn multi_row_values_placeholders(row_count: usize, cols_per_row: usize) -> String {
    (0..row_count)
        .map(|row| {
            let base = row * cols_per_row;
            let slots = (1..=cols_per_row)
                .map(|col| format!("${}", base + col))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({slots})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Absolute UPSERT into catalog Postgres `session_summary` (one shared SQL builder).
///
/// Column list / bind arity come from [`SESSION_SUMMARY_UPSERT_COLUMNS`] — do not
/// hard-code a column count elsewhere.
pub fn compile_session_summary_upsert_sql(schema_quoted: &str, row_count: usize) -> String {
    let cols = SESSION_SUMMARY_UPSERT_COLUMNS;
    let col_list = cols.join(", ");
    let values = multi_row_values_placeholders(row_count, cols.len());
    let set_list = cols
        .iter()
        .filter(|c| **c != "session_id")
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect::<Vec<_>>()
        .join(",\n           ");
    format!(
        "INSERT INTO {schema_quoted}.session_summary ({col_list}) VALUES {values} \
         ON CONFLICT (session_id) DO UPDATE SET \
           {set_list}"
    )
}

pub fn compile_session_summary_upsert_sql_for_workspace(
    schema_quoted: &str,
    row_count: usize,
) -> String {
    let cols = [
        "tenant_id",
        "session_id",
        "start_time",
        "end_time",
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
    let values = multi_row_values_placeholders(row_count, cols.len());
    let set_list = cols
        .iter()
        .filter(|column| **column != "tenant_id" && **column != "session_id")
        .map(|column| format!("{column} = EXCLUDED.{column}"))
        .collect::<Vec<_>>()
        .join(",\n           ");
    format!(
        "INSERT INTO {schema_quoted}.session_summary ({}) VALUES {values} \
         ON CONFLICT (tenant_id, session_id) DO UPDATE SET \
           {set_list}",
        cols.join(", ")
    )
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
        use crate::api::query_window::assert_sql_has_otlp_time_predicates;
        use crate::models::attr_keys::{enduser, sp};
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
        let cols = SESSION_SUMMARY_UPSERT_COLUMNS.len();
        let sql = compile_session_summary_upsert_sql("\"meta\"", 2);
        assert!(sql.contains("ON CONFLICT (session_id) DO UPDATE SET"));
        assert!(sql.contains("observation_count = EXCLUDED.observation_count"));
        assert!(
            sql.contains(&format!("${}", 2 * cols)),
            "2 rows × {cols} params; last placeholder missing in {sql}"
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
        assert!(reduce.contains("tenant_id = 'workspace''42'"));

        let upsert = compile_session_summary_upsert_sql_for_workspace("\"meta\"", 1);
        assert!(upsert.contains("(tenant_id, session_id) DO UPDATE"));
        assert!(upsert.contains("INSERT INTO \"meta\".session_summary (tenant_id, session_id"));
    }
}
