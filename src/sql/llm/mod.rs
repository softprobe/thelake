//! LLM OTLP query SQL recipes (one clock: `timestamp` only).

use crate::api::llm::query::{
    ObservationSearchRequest, SessionOrderBy, SessionSearchRequest, SortDirection,
};
use crate::api::sql_support::cursor_predicate;
use crate::sql::literal::sql_string_literal;
use crate::sql::QueryWindow;
use crate::storage::schema::variant::{
    prefer_attr_try_cast, prefer_attr_varchar, variant_as_json, variant_varchar,
};
use chrono::{DateTime, Utc};

pub const DEFAULT_SEARCH_LIMIT: usize = 50;
pub const DEFAULT_TRACE_LIMIT: usize = 100;
pub const DEFAULT_SESSION_LIMIT: usize = 50;
pub(crate) const MAX_LIMIT: usize = 200;

/// Softprobe product promotion column names from
/// `docs/promotion/traces-query-hot-attrs.yaml`.
///
/// Product list SQL COALESCE these ahead of the attribute MAP bag. Columns are
/// nullable, so the expression is safe before apply (all-NULL → bag fallback)
/// and fills after apply. Loading manifests on the query path is unnecessary.
///
/// Session-summary **reduce** uses the same names but **promoted-only** (no MAP).
#[derive(Debug, Clone, Copy)]
pub(crate) struct LlmAttrPromotions {
    pub(crate) observation_type: &'static str,
    pub(crate) model_name: &'static str,
    pub(crate) model_provider: &'static str,
    pub(crate) user_id: &'static str,
    pub(crate) input_tokens: &'static str,
    pub(crate) output_tokens: &'static str,
    pub(crate) total_tokens: &'static str,
    pub(crate) total_cost: &'static str,
}

impl LlmAttrPromotions {
    pub(crate) const PRODUCT: Self = Self {
        observation_type: "observation_type",
        model_name: "model_name",
        model_provider: "model_provider",
        user_id: "user_id",
        input_tokens: "input_tokens",
        output_tokens: "output_tokens",
        total_tokens: "total_tokens",
        total_cost: "total_cost",
    };

    /// Typed columns `session_summary.reduce` requires (product-hot subset).
    ///
    /// Single source for `hot_attrs::REQUIRED` — do not re-list these names elsewhere.
    /// `agent_name` is intentionally absent: auth-stamped / agent `message_type`, not yaml promote.
    pub(crate) const fn reduce_required_cols(&self) -> [&'static str; 7] {
        [
            self.observation_type,
            self.input_tokens,
            self.output_tokens,
            self.total_tokens,
            self.total_cost,
            self.user_id,
            self.model_name,
        ]
    }
}

pub(crate) fn llm_promo() -> LlmAttrPromotions {
    LlmAttrPromotions::PRODUCT
}

pub fn compile_session_recording_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let obs_type = format!("COALESCE({}, 'span')", expr_observation_type());
    let sid = sql_string_literal(session_id);
    Ok(window
        .bind_scan("", |bound| {
            format!(
                "SELECT {projection} FROM traces WHERE session_id = {sid} AND {obs_type} = 'recording' AND {bound} \
                 ORDER BY timestamp ASC, span_id ASC LIMIT {limit}",
                projection = observation_projection(true),
            )
        })
        .into_sql())
}

pub fn compile_session_search_sql(
    request: &SessionSearchRequest,
    limit: usize,
) -> Result<String, String> {
    let window = QueryWindow::try_new(request.from, request.to)?;

    let mut identity = vec![
        // Spans without a session id cannot belong to a session row.
        "session_id IS NOT NULL AND session_id <> ''".to_string(),
        // Web recording shares session_id with LLM spans but is not an LLM
        // observation — keep it off list aggregates / trace counts.
        exclude_recording_observation_sql(),
    ];
    if let Some(user_id) = request.user_id.as_deref().filter(|v| !v.trim().is_empty()) {
        identity.push(format!(
            "{} = {}",
            expr_user_id(),
            sql_string_literal(user_id)
        ));
    }
    if let Some(model) = request
        .model_name
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        identity.push(format!(
            "{} = {}",
            expr_model_name(),
            sql_string_literal(model)
        ));
    }

    // Cursor paging is defined against (start_time, session_id) descending.
    //
    // The predicate must sit on the OUTER query: start_time is an aggregate
    // alias (MIN(timestamp)), so pushing it into the inner WHERE both fails to
    // bind ("WHERE clause cannot contain aggregates") and would be wrong even
    // if it bound -- trimming raw spans by timestamp makes every SUM/COUNT for
    // that session cover only the post-cursor slice, so the aggregates would
    // shift as the caller pages.
    //
    // `order=asc` is rejected too: cursor_predicate emits `<`, which under an
    // ascending sort walks backwards and loops.
    //
    // Session-level `agent_name` is also an aggregate alias — filter it here.
    let mut outer_predicates = Vec::new();
    if let Some(cursor) = request.cursor.as_deref().filter(|v| !v.is_empty()) {
        if request.order_by != SessionOrderBy::StartTime {
            return Err("`cursor` is only supported with order_by=start_time".to_string());
        }
        if request.order != SortDirection::Desc {
            return Err("`cursor` is only supported with order=desc".to_string());
        }
        outer_predicates.push(cursor_predicate(cursor, "start_time", "session_id")?);
    }
    if let Some(agent) = request
        .agent_name
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        outer_predicates.push(format!("agent_name = {}", sql_string_literal(agent.trim())));
    }
    let cursor_sql = if outer_predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", outer_predicates.join(" AND "))
    };

    let mut having = Vec::new();
    if request.has_errors == Some(true) {
        having.push("error_count > 0".to_string());
    } else if request.has_errors == Some(false) {
        having.push("error_count = 0".to_string());
    }

    let observation_type = format!("COALESCE({}, 'span')", expr_observation_type());
    // Legacy OpenCode child sessions stamped their own ses_* as session_id and
    // marked the turn with parentSessionID. Drop those from the default list;
    // holistic (new) sessions keep a root agent turn without that metadata.
    // COALESCE inside arg_min matters: DuckDB arg_min skips NULLs, so a later
    // nested turn's parentSessionID would otherwise "win" over an earlier root.
    if request.roots_only {
        having.push(format!(
            "COALESCE(arg_min(COALESCE(CAST(attributes['sp.metadata.opencode.parentSessionID'] AS VARCHAR), ''), timestamp) FILTER (WHERE {obs_type} = 'agent'), '') = ''",
            obs_type = observation_type
        ));
    }

    let direction = request.order.as_sql();
    let order_sql = match request.order_by {
        SessionOrderBy::StartTime => format!("start_time {direction}, session_id {direction}"),
        SessionOrderBy::ErrorCount => {
            format!("error_count {direction}, start_time DESC, session_id DESC")
        }
        SessionOrderBy::Duration => {
            format!("duration_ms {direction}, start_time DESC, session_id DESC")
        }
        SessionOrderBy::TotalTokens => {
            format!("total_tokens {direction} NULLS LAST, start_time DESC, session_id DESC")
        }
        SessionOrderBy::TotalCost => {
            format!("total_cost {direction} NULLS LAST, start_time DESC, session_id DESC")
        }
    };

    let identity_sql = identity.join(" AND ");
    let having_sql = if having.is_empty() {
        String::new()
    } else {
        format!("HAVING {}", having.join(" AND "))
    };
    let fetch = limit + 1;

    Ok(window
        .bind_scan("", |bound| {
            format!(
                "SELECT * FROM ( \
           SELECT \
             session_id, \
             MIN(timestamp) AS start_time, \
             MAX(COALESCE(end_timestamp, timestamp)) AS end_time, \
             date_diff('millisecond', MIN(timestamp), MAX(COALESCE(end_timestamp, timestamp)))::BIGINT AS duration_ms, \
             COUNT(DISTINCT trace_id)::BIGINT AS trace_count, \
             COUNT(*)::BIGINT AS observation_count, \
             SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::BIGINT AS error_count, \
             SUM({input_tokens})::BIGINT AS input_tokens, \
             SUM({output_tokens})::BIGINT AS output_tokens, \
             SUM({total_tokens})::BIGINT AS total_tokens, \
             SUM({total_cost}) AS total_cost, \
             COALESCE( \
               NULLIF(arg_min({agent_attr}, timestamp) FILTER (WHERE NULLIF({agent_attr}, '') IS NOT NULL), ''), \
               arg_min(message_type, timestamp) FILTER (WHERE {obs_type} = 'agent') \
             ) AS agent_name, \
             list(DISTINCT {user_id}) AS user_ids, \
             list(DISTINCT {model_name}) AS models \
           FROM traces \
           WHERE {identity_sql} AND {bound} \
           GROUP BY session_id \
           {having_sql} \
         ){cursor_sql} \
         ORDER BY {order_sql} \
         LIMIT {fetch}",
                input_tokens = expr_input_tokens(),
                output_tokens = expr_output_tokens(),
                total_tokens = expr_total_tokens(),
                total_cost = expr_total_cost(),
                agent_attr = expr_agent_name_attr(),
                obs_type = observation_type,
                user_id = expr_user_id(),
                model_name = expr_model_name(),
            )
        })
        .into_sql())
}

pub fn compile_observation_search_sql(
    request: &ObservationSearchRequest,
) -> Result<String, String> {
    let window = QueryWindow::try_new(request.from, request.to)?;
    let limit = clamp_limit(request.limit, DEFAULT_SEARCH_LIMIT);
    let mut identity = Vec::new();

    if !request.observation_types.is_empty() {
        let values = request
            .observation_types
            .iter()
            .map(|value| sql_string_literal(value))
            .collect::<Vec<_>>()
            .join(", ");
        identity.push(format!(
            "COALESCE({}, 'span') IN ({values})",
            expr_observation_type()
        ));
    }
    if let Some(model_name) = &request.model_name {
        identity.push(format!(
            "{} = {}",
            expr_model_name(),
            sql_string_literal(model_name)
        ));
    }
    if let Some(user_id) = &request.user_id {
        identity.push(format!(
            "({sp} = {id} OR {enduser} = {id})",
            sp = prefer_attr_varchar(Some(llm_promo().user_id), "attributes", "sp.user.id"),
            enduser = variant_varchar("attributes", "enduser.id"),
            id = sql_string_literal(user_id)
        ));
    }
    if let Some(session_id) = &request.session_id {
        identity.push(format!("session_id = {}", sql_string_literal(session_id)));
    }
    if let Some(trace_id) = &request.trace_id {
        identity.push(format!("trace_id = {}", sql_string_literal(trace_id)));
    }

    let cursor_sql = match &request.cursor {
        Some(cursor) => Some(cursor_predicate(cursor, "timestamp", "span_id")?),
        None => None,
    };
    let fetch = limit + 1;
    let identity_sql = if identity.is_empty() {
        String::new()
    } else {
        format!("{} AND ", identity.join(" AND "))
    };

    Ok(window
        .bind_scan("", |bound| {
            let mut where_sql = format!("{identity_sql}{bound}");
            if let Some(ref cursor) = cursor_sql {
                where_sql = format!("{where_sql} AND {cursor}");
            }
            format!(
                "SELECT {projection} FROM traces WHERE {where_sql} ORDER BY timestamp DESC, span_id DESC LIMIT {fetch}",
                projection = observation_projection(false),
            )
        })
        .into_sql())
}

pub fn compile_observation_detail_sql(
    span_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(span_id);
    Ok(window
        .bind_scan("", |bound| {
            format!(
                "SELECT {projection} FROM traces WHERE span_id = {sid} AND {bound} LIMIT 1",
                projection = observation_projection(true),
            )
        })
        .into_sql())
}

pub fn compile_trace_summary_sql(
    trace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    session_id: Option<&str>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let mut identity = vec![format!("trace_id = {}", sql_string_literal(trace_id))];
    if let Some(session_id) = session_id.map(str::trim).filter(|v| !v.is_empty()) {
        identity.push(format!("session_id = {}", sql_string_literal(session_id)));
    }
    let identity_sql = identity.join(" AND ");
    Ok(window
        .bind_scan("", |bound| {
            format!(
                "SELECT {projection} FROM traces WHERE {identity_sql} AND {bound} GROUP BY trace_id",
                projection = trace_summary_projection(),
            )
        })
        .into_sql())
}

pub fn compile_trace_observations_sql(
    trace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    cursor: Option<&str>,
    session_id: Option<&str>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let mut identity = vec![format!("trace_id = {}", sql_string_literal(trace_id))];
    if let Some(session_id) = session_id.map(str::trim).filter(|v| !v.is_empty()) {
        identity.push(format!("session_id = {}", sql_string_literal(session_id)));
    }
    let cursor_sql = match cursor {
        Some(c) => Some(cursor_predicate(c, "timestamp", "span_id")?),
        None => None,
    };
    let identity_sql = identity.join(" AND ");
    let fetch = limit + 1;
    Ok(window
        .bind_scan("", |bound| {
            let mut where_sql = format!("{identity_sql} AND {bound}");
            if let Some(ref c) = cursor_sql {
                where_sql = format!("{where_sql} AND {c}");
            }
            format!(
                "SELECT {projection} FROM traces WHERE {where_sql} ORDER BY timestamp DESC, span_id DESC LIMIT {fetch}",
                projection = observation_projection(true),
            )
        })
        .into_sql())
}

pub fn compile_session_observations_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(session_id);
    let exclude = exclude_recording_observation_sql();
    let cursor_sql = match cursor {
        Some(c) => Some(cursor_predicate(c, "timestamp", "span_id")?),
        None => None,
    };
    let fetch = limit + 1;
    Ok(window
        .bind_scan("", |bound| {
            let mut where_sql = format!("session_id = {sid} AND {exclude} AND {bound}");
            if let Some(ref c) = cursor_sql {
                where_sql = format!("{where_sql} AND {c}");
            }
            format!(
                "SELECT {projection} FROM traces WHERE {where_sql} ORDER BY timestamp DESC, span_id DESC LIMIT {fetch}",
                // Product session detail (Explorer trajectory) needs sp.input / sp.output
                // from attributes — skinny list left the page empty. Expand-on-demand was
                // never wired on ProductSessionDetailView.
                projection = observation_projection(true),
            )
        })
        .into_sql())
}

pub fn compile_session_aggregate_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(session_id);
    let exclude = exclude_recording_observation_sql();
    Ok(window
        .bind_scan("", |bound| {
            format!(
                "SELECT \
            COUNT(DISTINCT trace_id) AS trace_count, \
            COUNT(*) AS observation_count, \
            SUM({input_tokens}) AS input_tokens, \
            SUM({output_tokens}) AS output_tokens, \
            SUM({total_tokens}) AS total_tokens, \
            SUM({total_cost}) AS total_cost, \
            list(DISTINCT {user_id}) AS user_ids \
         FROM traces \
         WHERE session_id = {sid} AND {exclude} AND {bound}",
                input_tokens = expr_input_tokens(),
                output_tokens = expr_output_tokens(),
                total_tokens = expr_total_tokens(),
                total_cost = expr_total_cost(),
                user_id = expr_user_id(),
            )
        })
        .into_sql())
}

pub fn compile_session_traces_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(session_id);
    let exclude = exclude_recording_observation_sql();
    let outer_cursor = match cursor {
        Some(c) => Some(cursor_predicate(c, "start_time", "trace_id")?),
        None => None,
    };
    let fetch = limit + 1;
    Ok(window
        .bind_scan("", |bound| {
            // Cursor applies to aggregated start_time/trace_id, so filter after GROUP BY.
            let inner = format!(
                "SELECT {projection} FROM traces WHERE session_id = {sid} AND {exclude} AND {bound} GROUP BY trace_id",
                projection = trace_summary_projection(),
            );
            let outer = match &outer_cursor {
                Some(c) => format!(" WHERE {c}"),
                None => String::new(),
            };
            format!(
                "SELECT * FROM ({inner}) AS t{outer} ORDER BY start_time DESC, trace_id DESC LIMIT {fetch}",
            )
        })
        .into_sql())
}

/// Recording spans share `session_id` with LLM work but must not inflate
/// session list / detail LLM aggregates or crowd out conversation traces.
fn exclude_recording_observation_sql() -> String {
    format!("COALESCE({}, '') <> 'recording'", expr_observation_type())
}

pub fn compile_scores_for_span_sql(
    span_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(span_id);
    Ok(window
        .bind_scan_timestamptz("", |bound| {
            format!(
                "SELECT {cols} FROM scores WHERE span_id = {sid} AND {bound} ORDER BY timestamp DESC, score_id DESC",
                cols = score_columns(),
            )
        })
        .into_sql())
}

pub fn compile_scores_for_trace_sql(
    trace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let tid = sql_string_literal(trace_id);
    Ok(window
        .bind_scan_ns_and_tz("", |ns_bound, tz_bound| {
            let identity = format!(
                "(trace_id = {tid} OR span_id IN (SELECT span_id FROM traces WHERE trace_id = {tid} AND {ns_bound}))"
            );
            format!(
                "SELECT {cols} FROM scores WHERE {identity} AND {tz_bound} ORDER BY timestamp DESC, score_id DESC",
                cols = score_columns(),
            )
        })
        .into_sql())
}

pub fn compile_scores_for_session_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(session_id);
    Ok(window
        .bind_scan_ns_and_tz("", |ns_bound, tz_bound| {
            let identity = format!(
                "(session_id = {sid} \
         OR trace_id IN (SELECT DISTINCT trace_id FROM traces WHERE session_id = {sid} AND {ns_bound}) \
         OR span_id IN (SELECT span_id FROM traces WHERE session_id = {sid} AND {ns_bound}))"
            );
            format!(
                "SELECT {cols} FROM scores WHERE {identity} AND {tz_bound} ORDER BY timestamp DESC, score_id DESC",
                cols = score_columns(),
            )
        })
        .into_sql())
}

fn observation_projection(include_payload: bool) -> String {
    let mut cols = vec![
        "trace_id".to_string(),
        "span_id".to_string(),
        "parent_span_id".to_string(),
        "NULLIF(session_id, '') AS session_id".to_string(),
        "message_type AS name".to_string(),
        format!(
            "COALESCE({}, 'span') AS observation_type",
            expr_observation_type()
        ),
        "timestamp AS start_time".to_string(),
        "end_timestamp AS end_time".to_string(),
        "status_code".to_string(),
        format!("{} AS model_name", expr_model_name()),
        format!("{} AS model_provider", expr_model_provider()),
        format!("{} AS user_id", expr_user_id()),
        format!("{} AS input_tokens", expr_input_tokens()),
        format!("{} AS output_tokens", expr_output_tokens()),
        format!("{} AS total_tokens", expr_total_tokens()),
        format!("{} AS total_cost", expr_total_cost()),
    ];
    if include_payload {
        cols.push(variant_as_json("attributes"));
        cols.push("events".to_string());
    }
    cols.join(", ")
}

fn trace_summary_projection() -> String {
    format!(
        "trace_id, \
         any_value(NULLIF(session_id, '')) AS session_id, \
         any_value(message_type) AS name, \
         MIN(timestamp) AS start_time, \
         MAX(COALESCE(end_timestamp, timestamp)) AS end_time, \
         COUNT(*)::BIGINT AS observation_count, \
         SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END)::BIGINT AS error_count, \
         SUM({input_tokens})::BIGINT AS input_tokens, \
         SUM({output_tokens})::BIGINT AS output_tokens, \
         SUM({total_tokens})::BIGINT AS total_tokens, \
         SUM({total_cost}) AS total_cost, \
         any_value({user_id}) AS user_id",
        input_tokens = expr_input_tokens(),
        output_tokens = expr_output_tokens(),
        total_tokens = expr_total_tokens(),
        total_cost = expr_total_cost(),
        user_id = expr_user_id(),
    )
}

pub(crate) fn expr_observation_type() -> String {
    prefer_attr_varchar(
        Some(llm_promo().observation_type),
        "attributes",
        crate::models::attr_keys::sp::OBSERVATION_TYPE,
    )
}

fn expr_model_name() -> String {
    prefer_attr_varchar(
        Some(llm_promo().model_name),
        "attributes",
        crate::models::attr_keys::gen_ai::REQUEST_MODEL,
    )
}

fn expr_model_provider() -> String {
    prefer_attr_varchar(
        Some(llm_promo().model_provider),
        "attributes",
        crate::models::attr_keys::gen_ai::PROVIDER_NAME,
    )
}

fn expr_user_id() -> String {
    // enduser.id is bag-only fallback (not in product hot-attrs manifest).
    format!(
        "COALESCE({}, {})",
        prefer_attr_varchar(
            Some(llm_promo().user_id),
            "attributes",
            crate::models::attr_keys::sp::USER_ID,
        ),
        variant_varchar("attributes", "enduser.id")
    )
}

/// Session agent name: persisted assertion column, then `sp.agent.name`, else bag-only.
fn expr_agent_name_attr() -> String {
    prefer_attr_varchar(
        Some("agent_name"),
        "attributes",
        crate::models::attr_keys::sp::AGENT_NAME,
    )
}

fn expr_input_tokens() -> String {
    prefer_attr_try_cast(
        Some(llm_promo().input_tokens),
        "attributes",
        crate::models::attr_keys::gen_ai::USAGE_INPUT_TOKENS,
        "BIGINT",
    )
}

fn expr_output_tokens() -> String {
    prefer_attr_try_cast(
        Some(llm_promo().output_tokens),
        "attributes",
        crate::models::attr_keys::gen_ai::USAGE_OUTPUT_TOKENS,
        "BIGINT",
    )
}

fn expr_total_tokens() -> String {
    prefer_attr_try_cast(
        Some(llm_promo().total_tokens),
        "attributes",
        crate::models::attr_keys::gen_ai::USAGE_TOTAL_TOKENS,
        "BIGINT",
    )
}

fn expr_total_cost() -> String {
    prefer_attr_try_cast(
        Some(llm_promo().total_cost),
        "attributes",
        crate::models::attr_keys::sp::COST_TOTAL,
        "DOUBLE",
    )
}

fn score_columns() -> &'static str {
    "score_id, timestamp, trace_id, span_id, session_id, name, data_type, numeric_value, string_value, boolean_value, source, comment, config_id, author_id, metadata"
}

pub fn clamp_limit(limit: Option<usize>, default: usize) -> usize {
    limit.unwrap_or(default).clamp(1, MAX_LIMIT)
}
