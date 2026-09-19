//! Promoted-only DuckLake aggregate SQL for `session_summary.reduce`.
//!
//! **Hard rule:** never reference `attributes` / MAP bags. List path may use
//! `prefer_attr_*`; reduce must not.

use crate::api::llm::query::llm_promo;
use crate::api::sql_support::{sql_string_literal, timestamp_ns_column, timestamp_ns_literal};
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
/// `from_table` is typically `traces` (unit tests) or a DuckLake-qualified name.
pub fn compile_session_summary_reduce_sql(
    from_table: &str,
    session_ids: &[String],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String> {
    if session_ids.is_empty() {
        bail!("session_summary reduce SQL requires at least one session_id");
    }
    if from > to {
        bail!("session_summary reduce SQL requires from <= to");
    }
    if from_table.is_empty() || from_table.contains(';') {
        bail!("invalid from_table for reduce SQL");
    }

    let promo = llm_promo();
    let from_date = from.date_naive();
    let to_date = to.date_naive();
    let in_list = session_ids
        .iter()
        .map(|id| sql_string_literal(id))
        .collect::<Vec<_>>()
        .join(", ");

    // Predicate order: record_date → session_id → timestamp → exclude recording.
    let where_sql = format!(
        "record_date BETWEEN DATE '{from_date}' AND DATE '{to_date}' \
         AND session_id IN ({in_list}) \
         AND session_id <> '' \
         AND {ts_col} >= {from_ts} \
         AND {ts_col} <= {to_ts} \
         AND COALESCE({obs}, '') <> 'recording'",
        ts_col = timestamp_ns_column("timestamp"),
        from_ts = timestamp_ns_literal(&from),
        to_ts = timestamp_ns_literal(&to),
        obs = promo.observation_type,
    );

    // Typed agent resolve (no attributes['sp.agent.name']).
    let agent_expr = format!(
        "COALESCE( \
           NULLIF(arg_min(agent_name, timestamp) FILTER (WHERE NULLIF(agent_name, '') IS NOT NULL), ''), \
           arg_min(message_type, timestamp) FILTER (WHERE COALESCE({obs}, '') = 'agent') \
         )",
        obs = promo.observation_type,
    );

    Ok(format!(
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
           {agent} AS agent_name, \
           arg_min({user_id}, timestamp) FILTER (WHERE NULLIF({user_id}, '') IS NOT NULL) AS user_id, \
           arg_min({model_name}, timestamp) FILTER (WHERE NULLIF({model_name}, '') IS NOT NULL) AS model_name \
         FROM {from_table} \
         WHERE {where_sql} \
         GROUP BY session_id",
        input_tokens = promo.input_tokens,
        output_tokens = promo.output_tokens,
        total_tokens = promo.total_tokens,
        total_cost = promo.total_cost,
        agent = agent_expr,
        user_id = promo.user_id,
        model_name = promo.model_name,
        from_table = from_table,
        where_sql = where_sql,
    ))
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

    #[test]
    fn reduce_sql_has_pushdown_and_no_attributes() {
        let (from, to) = sample_window();
        let sql =
            compile_session_summary_reduce_sql("traces", &["s1".into(), "s2".into()], from, to)
                .expect("sql");
        assert!(sql.contains("record_date BETWEEN"));
        assert!(sql.contains("session_id IN"));
        assert!(sql.contains("TIMESTAMP_NS"));
        assert!(sql.contains("COUNT(DISTINCT span_id)"));
        assert!(sql.contains("COALESCE(observation_type, '') <> 'recording'"));
        assert!(sql.contains("SUM(input_tokens)"));
        assert!(sql.contains("SUM(total_cost)"));
        assert!(!sql.to_lowercase().contains("attributes"));
        assert!(!sql.contains("SELECT *"));
        assert!(!sql.contains("events"));
        assert!(!sql.contains("http_request_body"));
        assert!(!sql.contains("http_response_body"));
        assert!(!sql.contains("resource_attributes"));
        // record_date before session_id before timestamp
        let rd = sql.find("record_date").expect("record_date");
        let sid = sql.find("session_id IN").expect("session_id IN");
        let ts = sql.find("CAST(timestamp AS TIMESTAMP_NS)").expect("ts");
        assert!(
            rd < sid && sid < ts,
            "predicate order must be prune-friendly"
        );
    }

    #[test]
    fn reduce_sql_rejects_empty_ids() {
        let (from, to) = sample_window();
        assert!(compile_session_summary_reduce_sql("traces", &[], from, to).is_err());
    }

    #[test]
    fn reduce_sql_rejects_inverted_range() {
        let (from, to) = sample_window();
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
}
