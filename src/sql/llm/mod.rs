//! LLM OTLP query SQL recipes (one clock: `timestamp` only).

pub mod search;

pub use search::{
    SessionDetail, SpanDetail, SpanSearchRequest, SpanSearchResponse, SpanSummary, Trace,
    TraceDetail,
};

use crate::session_summary::list_query::{SessionOrderBy, SessionSearchRequest, SortDirection};
use crate::sql::literal::sql_string_literal;
use crate::sql::paging::cursor_predicate;
use crate::sql::QueryWindow;
use crate::storage::schema::attribute_map::{
    attribute_map_as_json, attribute_map_try_cast, attribute_map_varchar, prefer_attr_try_cast,
};
use chrono::{DateTime, Utc};

const SESSION_DETAIL_SQL: &str = include_str!("session_detail.sql");
const SESSION_SEARCH_SQL: &str = include_str!("session_search.sql");
const TRACE_SUMMARY_SQL: &str = include_str!("trace_summary.sql");
const TRACE_SPANS_SQL: &str = include_str!("trace_spans.sql");
const SPAN_SEARCH_SQL: &str = include_str!("span_search.sql");

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
        .scan_with_timestamp_filter("", |bound| {
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
    let session_id_expr = expr_session_id();
    let identity = [
        format!("{session_id_expr} IS NOT NULL"),
        exclude_recording_observation_sql(),
    ];

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

    let mut having = vec!["COUNT(*) FILTER (WHERE obs_type <> 'http_body') > 0".to_string()];
    if let Some(user_id) = request.user_id.as_deref().filter(|v| !v.trim().is_empty()) {
        having.push(format!(
            "list_contains(list(DISTINCT user_id), {})",
            sql_string_literal(user_id.trim())
        ));
    }
    if let Some(model) = request
        .model_name
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        having.push(format!(
            "list_contains(list(DISTINCT model_name), {})",
            sql_string_literal(model.trim())
        ));
    }
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
        having.push(
            "COALESCE(arg_min(parent_session_id, timestamp) FILTER (WHERE obs_type = 'agent'), '') = ''"
                .to_string(),
        );
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
    let having_sql = format!("HAVING {}", having.join(" AND "));
    let fetch = (limit + 1).to_string();
    let agent_attr = expr_agent_name_attr();
    let input_tokens = expr_input_tokens();
    let output_tokens = expr_output_tokens();
    let total_tokens = expr_total_tokens();
    let total_cost = expr_total_cost();
    let user_id = expr_user_id();
    let model_name = expr_model_name();

    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            SESSION_SEARCH_SQL
                .replace("{{session_id_expr}}", &session_id_expr)
                .replace("{{obs_type_expr}}", &observation_type)
                .replace("{{input_tokens_expr}}", &input_tokens)
                .replace("{{output_tokens_expr}}", &output_tokens)
                .replace("{{total_tokens_expr}}", &total_tokens)
                .replace("{{total_cost_expr}}", &total_cost)
                .replace("{{agent_attr_expr}}", &agent_attr)
                .replace("{{user_id_expr}}", &user_id)
                .replace("{{model_name_expr}}", &model_name)
                .replace("{{identity_sql}}", &identity_sql)
                .replace("{{timestamp_filter}}", bound)
                .replace("{{having_sql}}", &having_sql)
                .replace("{{cursor_sql}}", &cursor_sql)
                .replace("{{order_sql}}", &order_sql)
                .replace("{{fetch}}", &fetch)
        })
        .into_sql())
}

fn approve(sql: String) -> Result<crate::sql::trusted::TrustedSql, String> {
    crate::sql::trusted::approved_query(sql).map_err(|error| error.to_string())
}

/// Compile and approve the LLM span-search recipe for trusted execution.
pub(crate) fn search_spans(
    request: &SpanSearchRequest,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_span_search_sql(request)?)
}

/// Compile and approve span detail for trusted execution.
pub(crate) fn span_detail(
    span_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_span_detail_sql(span_id, from, to)?)
}

/// Compile and approve trace summary for trusted execution.
pub(crate) fn trace_summary(
    trace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    session_id: Option<&str>,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_trace_summary_sql(trace_id, from, to, session_id)?)
}

/// Compile and approve trace spans list for trusted execution.
pub(crate) fn trace_spans(
    trace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
    cursor: Option<&str>,
    session_id: Option<&str>,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_trace_spans_sql(
        trace_id, from, to, limit, cursor, session_id,
    )?)
}

/// Compile and approve session detail for trusted execution.
pub(crate) fn session_detail(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_session_detail_sql(session_id, from, to)?)
}

/// Compile and approve session recording for trusted execution.
pub(crate) fn session_recording(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: usize,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_session_recording_sql(session_id, from, to, limit)?)
}

/// Compile and approve scores-for-span for trusted execution.
pub(crate) fn scores_for_span(
    span_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_scores_for_span_sql(span_id, from, to)?)
}

/// Compile and approve scores-for-trace for trusted execution.
pub(crate) fn scores_for_trace(
    trace_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<crate::sql::trusted::TrustedSql, String> {
    approve(compile_scores_for_trace_sql(trace_id, from, to)?)
}

pub fn compile_span_search_sql(request: &SpanSearchRequest) -> Result<String, String> {
    let window = QueryWindow::try_new(request.from, request.to)?;
    let limit = clamp_limit(request.limit, DEFAULT_SEARCH_LIMIT);
    let mut identity = Vec::new();

    if !request.span_types.is_empty() {
        let values = request
            .span_types
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
            "{} = {}",
            expr_user_id(),
            sql_string_literal(user_id)
        ));
    }
    if let Some(session_id) = &request.session_id {
        identity.push(format!(
            "{} = {}",
            expr_session_id(),
            sql_string_literal(session_id)
        ));
    }
    if let Some(trace_id) = &request.trace_id {
        identity.push(format!("trace_id = {}", sql_string_literal(trace_id)));
    }

    let cursor_suffix = match &request.cursor {
        Some(cursor) => format!(" AND {}", cursor_predicate(cursor, "timestamp", "span_id")?),
        None => String::new(),
    };
    let fetch = (limit + 1).to_string();
    let identity_prefix = if identity.is_empty() {
        String::new()
    } else {
        format!("{} AND ", identity.join(" AND "))
    };
    let projection = observation_projection(false);

    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            SPAN_SEARCH_SQL
                .replace("{{projection}}", &projection)
                .replace("{{identity_prefix}}", &identity_prefix)
                .replace("{{timestamp_filter}}", bound)
                .replace("{{cursor_suffix}}", &cursor_suffix)
                .replace("{{fetch}}", &fetch)
        })
        .into_sql())
}

pub fn compile_span_detail_sql(
    span_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(span_id);
    Ok(window
        .scan_with_timestamp_filter("", |bound| {
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
    let session_id_expr = expr_session_id();
    let mut identity = vec![format!("trace_id = {}", sql_string_literal(trace_id))];
    if let Some(session_id) = session_id.map(str::trim).filter(|v| !v.is_empty()) {
        identity.push(format!(
            "{session_id_expr} = {}",
            sql_string_literal(session_id)
        ));
    }
    let identity_sql = identity.join(" AND ");
    let input_tokens = expr_input_tokens();
    let output_tokens = expr_output_tokens();
    let total_tokens = expr_total_tokens();
    let total_cost = expr_total_cost();
    let user_id = expr_user_id();
    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            TRACE_SUMMARY_SQL
                .replace("{{session_id_expr}}", &session_id_expr)
                .replace("{{input_tokens_expr}}", &input_tokens)
                .replace("{{output_tokens_expr}}", &output_tokens)
                .replace("{{total_tokens_expr}}", &total_tokens)
                .replace("{{total_cost_expr}}", &total_cost)
                .replace("{{user_id_expr}}", &user_id)
                .replace("{{identity_sql}}", &identity_sql)
                .replace("{{timestamp_filter}}", bound)
        })
        .into_sql())
}

pub fn compile_trace_spans_sql(
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
        identity.push(format!(
            "{} = {}",
            expr_session_id(),
            sql_string_literal(session_id)
        ));
    }
    let cursor_suffix = match cursor {
        Some(c) => format!(" AND {}", cursor_predicate(c, "timestamp", "span_id")?),
        None => String::new(),
    };
    let identity_sql = identity.join(" AND ");
    let fetch = (limit + 1).to_string();
    let projection = observation_projection(true);
    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            TRACE_SPANS_SQL
                .replace("{{projection}}", &projection)
                .replace("{{identity_sql}}", &identity_sql)
                .replace("{{timestamp_filter}}", bound)
                .replace("{{cursor_suffix}}", &cursor_suffix)
                .replace("{{fetch}}", &fetch)
        })
        .into_sql())
}

/// Full span rows and whole-session metrics for one product session.
///
/// The materialized session relation is shared by the span projection and
/// aggregate, so session detail does not issue separate lake scans for its
/// rows and totals. This deliberately returns the complete session; the
/// detail API is one holistic read rather than a client-drained cursor.
pub fn compile_session_detail_sql(
    session_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<String, String> {
    let window = QueryWindow::try_new(from, to)?;
    let sid = sql_string_literal(session_id);
    let session_predicate = format!("{} = {sid}", expr_session_id());
    let exclude = exclude_recording_observation_sql();
    let projection = observation_projection(true);
    let scores_proj = score_columns("CAST(metadata AS JSON)");
    Ok(window
        .scan_with_timestamp_filter("", |bound| {
            SESSION_DETAIL_SQL
                .replace("{{projection}}", &projection)
                .replace("{{session_predicate}}", &session_predicate)
                .replace("{{exclude_recording}}", &exclude)
                .replace("{{timestamp_filter}}", bound)
                .replace("{{score_columns}}", &scores_proj)
                .replace("{{session_id_literal}}", &sid)
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
        .scan_with_timestamp_filter("", |bound| {
            format!(
                "SELECT {cols} FROM scores WHERE span_id = {sid} AND {bound} ORDER BY timestamp DESC, score_id DESC",
                cols = score_columns("metadata"),
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
        .scan_with_timestamp_filter("", |bound| {
            let identity = format!(
                "(trace_id = {tid} OR span_id IN (SELECT span_id FROM traces WHERE trace_id = {tid} AND {bound}))"
            );
            format!(
                "SELECT {cols} FROM scores WHERE {identity} AND {bound} ORDER BY timestamp DESC, score_id DESC",
                cols = score_columns("metadata"),
            )
        })
        .into_sql())
}

fn observation_projection(include_payload: bool) -> String {
    let mut cols = vec![
        "trace_id".to_string(),
        "span_id".to_string(),
        "parent_span_id".to_string(),
        format!("{} AS session_id", expr_session_id()),
        "message_type AS name".to_string(),
        format!("COALESCE({}, 'span') AS span_type", expr_observation_type()),
        format!("{} AS service_name", expr_service_name()),
        format!("{} AS span_kind", expr_span_kind()),
        "timestamp AS start_time".to_string(),
        "end_timestamp AS end_time".to_string(),
        "status_code".to_string(),
        "NULLIF(TRIM(status_message), '') AS status_message".to_string(),
        format!("{} AS model_name", expr_model_name()),
        format!("{} AS model_provider", expr_model_provider()),
        format!("{} AS user_id", expr_user_id()),
        format!("{} AS input_tokens", expr_input_tokens()),
        format!("{} AS output_tokens", expr_output_tokens()),
        format!("{} AS total_tokens", expr_total_tokens()),
        format!("{} AS total_cost", expr_total_cost()),
    ];
    if include_payload {
        cols.push("NULLIF(TRIM(app_id), '') AS app_id".to_string());
        cols.push(attribute_map_as_json("attributes"));
        cols.push(attribute_map_as_json("resource_attributes"));
        cols.push("events".to_string());
    }
    cols.join(", ")
}

fn non_empty_col(col: &str) -> String {
    format!("NULLIF(TRIM({col}), '')")
}

fn non_empty_attr(bag: &str, key: &str) -> String {
    non_empty_col(&attribute_map_varchar(bag, key))
}

fn coalesce_exprs(exprs: &[String]) -> String {
    format!("COALESCE({})", exprs.join(", "))
}

fn expr_service_name() -> String {
    use crate::models::attr_keys::resource;
    coalesce_exprs(&[
        non_empty_col("service_name"),
        non_empty_attr("resource_attributes", resource::SERVICE_NAME),
        non_empty_attr("attributes", resource::SERVICE_NAME),
        non_empty_col("app_id"),
    ])
}

fn expr_span_kind() -> String {
    "CASE UPPER(TRIM(COALESCE(span_kind, ''))) \
       WHEN 'INTERNAL' THEN 'INTERNAL' \
       WHEN 'SPAN_KIND_INTERNAL' THEN 'INTERNAL' \
       WHEN '1' THEN 'INTERNAL' \
       WHEN 'SERVER' THEN 'SERVER' \
       WHEN 'SPAN_KIND_SERVER' THEN 'SERVER' \
       WHEN '2' THEN 'SERVER' \
       WHEN 'CLIENT' THEN 'CLIENT' \
       WHEN 'SPAN_KIND_CLIENT' THEN 'CLIENT' \
       WHEN '3' THEN 'CLIENT' \
       WHEN 'PRODUCER' THEN 'PRODUCER' \
       WHEN 'SPAN_KIND_PRODUCER' THEN 'PRODUCER' \
       WHEN '4' THEN 'PRODUCER' \
       WHEN 'CONSUMER' THEN 'CONSUMER' \
       WHEN 'SPAN_KIND_CONSUMER' THEN 'CONSUMER' \
       WHEN '5' THEN 'CONSUMER' \
       ELSE NULL END"
        .to_string()
}

pub(crate) fn expr_session_id() -> String {
    use crate::models::attr_keys::{gen_ai, otel, sp, traceloop, vercel_ai};
    coalesce_exprs(&[
        "NULLIF(session_attr_id, '')".to_string(),
        non_empty_attr("attributes", sp::SESSION_ID),
        non_empty_attr("attributes", sp::SESSION_ID_UNDERSCORE),
        non_empty_attr("attributes", otel::SESSION_ID),
        non_empty_attr("attributes", gen_ai::CONVERSATION_ID),
        non_empty_attr("attributes", vercel_ai::SESSION_ID),
        non_empty_attr("attributes", vercel_ai::CONVERSATION_ID),
        non_empty_attr("attributes", traceloop::SESSION_ID),
        non_empty_col("session_id"),
    ])
}

pub(crate) fn expr_observation_type() -> String {
    use crate::models::attr_keys::{gen_ai, openinference, sp, traceloop, vercel_ai};
    let gen_ai_op = format!(
        "LOWER(COALESCE({}, ''))",
        non_empty_attr("attributes", gen_ai::OPERATION_NAME)
    );
    let oi_kind = format!(
        "UPPER(COALESCE({}, ''))",
        non_empty_attr("attributes", openinference::SPAN_KIND)
    );
    let tl_kind = format!(
        "LOWER(COALESCE({}, ''))",
        non_empty_attr("attributes", traceloop::SPAN_KIND)
    );
    let ai_op = format!(
        "COALESCE({}, '')",
        non_empty_attr("attributes", vercel_ai::OPERATION_ID)
    );
    // Split across CONCAT so TrustedSql::validate_for_scope does not mistake the
    // OBI span-name literal for the default `softprobe` catalog or schema identifier.
    let body_name = "CONCAT('soft', 'probe.http.body.capture')";

    coalesce_exprs(&[
        llm_promo().observation_type.to_string(),
        non_empty_attr("attributes", sp::OBSERVATION_TYPE),
        format!("CASE WHEN TRIM(message_type) = {body_name} THEN 'http_body' ELSE NULL END"),
        format!(
            "CASE {gen_ai_op} \
               WHEN 'chat' THEN 'generation' \
               WHEN 'generate_content' THEN 'generation' \
               WHEN 'text_completion' THEN 'generation' \
               WHEN 'embeddings' THEN 'embedding' \
               WHEN 'execute_tool' THEN 'tool' \
               WHEN 'invoke_agent' THEN 'agent' \
               WHEN 'create_agent' THEN 'agent' \
               WHEN 'invoke_workflow' THEN 'agent' \
               ELSE NULL END"
        ),
        format!(
            "CASE {oi_kind} \
               WHEN 'LLM' THEN 'generation' \
               WHEN 'EMBEDDING' THEN 'embedding' \
               WHEN 'TOOL' THEN 'tool' \
               WHEN 'AGENT' THEN 'agent' \
               WHEN 'CHAIN' THEN 'chain' \
               WHEN 'RETRIEVER' THEN 'retriever' \
               WHEN 'RERANKER' THEN 'retriever' \
               WHEN 'GUARDRAIL' THEN 'guardrail' \
               WHEN 'EVALUATOR' THEN 'evaluator' \
               ELSE NULL END"
        ),
        format!(
            "CASE {tl_kind} \
               WHEN 'agent' THEN 'agent' \
               WHEN 'tool' THEN 'tool' \
               WHEN 'workflow' THEN 'chain' \
               WHEN 'task' THEN 'chain' \
               ELSE NULL END"
        ),
        format!(
            "CASE \
               WHEN {ai_op} = 'ai.toolCall' THEN 'tool' \
               WHEN starts_with({ai_op}, 'ai.embed') THEN 'embedding' \
               WHEN ends_with({ai_op}, '.doStream') OR ends_with({ai_op}, '.doGenerate') THEN 'generation' \
               WHEN {ai_op} IN ('ai.streamText', 'ai.generateText', 'ai.streamObject', 'ai.generateObject') THEN 'generation' \
               ELSE NULL END"
        ),
    ])
}

fn expr_model_name() -> String {
    use crate::models::attr_keys::{gen_ai, openinference, vercel_ai};
    coalesce_exprs(&[
        llm_promo().model_name.to_string(),
        non_empty_attr("attributes", gen_ai::REQUEST_MODEL),
        non_empty_attr("attributes", gen_ai::RESPONSE_MODEL),
        non_empty_attr("attributes", openinference::LLM_MODEL_NAME),
        non_empty_attr("attributes", vercel_ai::MODEL_ID),
    ])
}

fn expr_model_provider() -> String {
    use crate::models::attr_keys::{gen_ai, openinference, vercel_ai};
    coalesce_exprs(&[
        llm_promo().model_provider.to_string(),
        non_empty_attr("attributes", gen_ai::PROVIDER_NAME),
        non_empty_attr("attributes", gen_ai::SYSTEM),
        non_empty_attr("attributes", openinference::LLM_PROVIDER),
        non_empty_attr("attributes", openinference::LLM_SYSTEM),
        non_empty_attr("attributes", vercel_ai::MODEL_PROVIDER),
    ])
}

fn expr_user_id() -> String {
    use crate::models::attr_keys::{enduser, otel, sp, traceloop, vercel_ai};
    coalesce_exprs(&[
        llm_promo().user_id.to_string(),
        non_empty_attr("attributes", sp::USER_ID),
        non_empty_attr("attributes", otel::USER_ID),
        non_empty_attr("attributes", enduser::ID),
        non_empty_attr("attributes", vercel_ai::USER_ID),
        non_empty_attr("attributes", traceloop::USER_ID),
    ])
}

fn expr_agent_name_attr() -> String {
    use crate::models::attr_keys::{gen_ai, sp, traceloop};
    let obs = expr_observation_type();
    let entity = non_empty_attr("attributes", traceloop::ENTITY_NAME);
    coalesce_exprs(&[
        "NULLIF(TRIM(agent_name), '')".to_string(),
        non_empty_attr("attributes", gen_ai::AGENT_NAME),
        non_empty_attr("attributes", sp::AGENT_NAME),
        format!("CASE WHEN COALESCE({obs}, '') = 'agent' THEN {entity} ELSE NULL END"),
    ])
}

fn expr_input_tokens() -> String {
    use crate::models::attr_keys::{gen_ai, openinference, vercel_ai};
    coalesce_exprs(&[
        prefer_attr_try_cast(
            Some(llm_promo().input_tokens),
            "attributes",
            gen_ai::USAGE_INPUT_TOKENS,
            "BIGINT",
        ),
        attribute_map_try_cast("attributes", gen_ai::USAGE_PROMPT_TOKENS, "BIGINT"),
        attribute_map_try_cast("attributes", openinference::TOKEN_COUNT_PROMPT, "BIGINT"),
        attribute_map_try_cast("attributes", vercel_ai::USAGE_INPUT_TOKENS, "BIGINT"),
        attribute_map_try_cast("attributes", vercel_ai::USAGE_PROMPT_TOKENS, "BIGINT"),
    ])
}

fn expr_output_tokens() -> String {
    use crate::models::attr_keys::{gen_ai, openinference, vercel_ai};
    coalesce_exprs(&[
        prefer_attr_try_cast(
            Some(llm_promo().output_tokens),
            "attributes",
            gen_ai::USAGE_OUTPUT_TOKENS,
            "BIGINT",
        ),
        attribute_map_try_cast("attributes", gen_ai::USAGE_COMPLETION_TOKENS, "BIGINT"),
        attribute_map_try_cast(
            "attributes",
            openinference::TOKEN_COUNT_COMPLETION,
            "BIGINT",
        ),
        attribute_map_try_cast("attributes", vercel_ai::USAGE_OUTPUT_TOKENS, "BIGINT"),
        attribute_map_try_cast("attributes", vercel_ai::USAGE_COMPLETION_TOKENS, "BIGINT"),
    ])
}

fn expr_total_tokens() -> String {
    use crate::models::attr_keys::{gen_ai, openinference, vercel_ai};
    coalesce_exprs(&[
        prefer_attr_try_cast(
            Some(llm_promo().total_tokens),
            "attributes",
            gen_ai::USAGE_TOTAL_TOKENS,
            "BIGINT",
        ),
        attribute_map_try_cast("attributes", openinference::TOKEN_COUNT_TOTAL, "BIGINT"),
        attribute_map_try_cast("attributes", vercel_ai::USAGE_TOTAL_TOKENS, "BIGINT"),
    ])
}

fn expr_total_cost() -> String {
    prefer_attr_try_cast(
        Some(llm_promo().total_cost),
        "attributes",
        crate::models::attr_keys::sp::COST_TOTAL,
        "DOUBLE",
    )
}

fn score_columns(metadata: &str) -> String {
    format!(
        "score_id, timestamp, trace_id, span_id, session_id, name, data_type, numeric_value, \
         string_value, boolean_value, source, comment, config_id, author_id, {metadata} AS metadata"
    )
}

pub fn clamp_limit(limit: Option<usize>, default: usize) -> usize {
    limit.unwrap_or(default).clamp(1, MAX_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn payload_projection_reads_json_events_without_recasting() {
        let sql = observation_projection(true);
        assert!(
            sql.contains("events") && !sql.contains("CAST(events AS JSON)"),
            "events already use the JSON table type: {sql}"
        );
    }

    #[test]
    fn search_spans_trusted_wrapper_matches_compile() {
        let request = SpanSearchRequest {
            from: Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap(),
            span_types: vec!["generation".into()],
            model_name: None,
            user_id: None,
            session_id: None,
            trace_id: None,
            limit: Some(10),
            cursor: None,
        };
        let compiled = compile_span_search_sql(&request).expect("compile");
        let trusted = search_spans(&request).expect("approve");
        let default_scope = crate::storage::ducklake::PhysicalScope::from_ducklake(
            &crate::config::DuckLakeConfig::default(),
        );
        for q in [
            trusted.clone(),
            span_detail("sp1", request.from, request.to).expect("span_detail"),
            trace_summary("tr1", request.from, request.to, Some("ses1")).expect("trace_summary"),
            trace_spans("tr1", request.from, request.to, 10, None, Some("ses1"))
                .expect("trace_spans"),
            session_detail("ses1", request.from, request.to).expect("session_detail"),
            session_recording("ses1", request.from, request.to, 10).expect("session_recording"),
            scores_for_span("sp1", request.from, request.to).expect("scores_for_span"),
            scores_for_trace("tr1", request.from, request.to).expect("scores_for_trace"),
        ] {
            q.validate_for_scope(&default_scope).unwrap_or_else(|e| {
                panic!("scope validation failed ({e}) for SQL: {}", q.as_str())
            });
        }
        assert_eq!(trusted.as_str(), compiled);
        assert!(trusted.as_str().contains("timestamp >="));
        assert!(trusted.as_str().contains("timestamp <="));
    }

    fn open_duckdb_with_traces_and_scores() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().expect("in-memory duckdb");
        conn.execute_batch(include_str!("../schema/traces.sql"))
            .expect("create traces");
        conn.execute_batch(include_str!("../schema/scores.sql"))
            .expect("create scores");
        conn
    }

    #[test]
    fn historical_rows_without_promoted_columns_resolve_multi_standard_fields_and_service_fallback()
    {
        let conn = open_duckdb_with_traces_and_scores();
        conn.execute_batch(
            "INSERT INTO traces (
               session_id, trace_id, span_id, parent_span_id, app_id, message_type, span_kind,
               timestamp, end_timestamp, attributes, resource_attributes, events, status_code, status_message
             ) VALUES
             (
               't_hist_1', 't_hist_1', 'sp_agent', NULL, 'fallback-app', 'concierge.run', 'Internal',
               TIMESTAMP_NS '2026-07-18 12:00:00', TIMESTAMP_NS '2026-07-18 12:00:05',
               MAP {
                 'traceloop.span.kind': 'agent',
                 'traceloop.entity.name': 'concierge-agent',
                 'traceloop.association.properties.session_id': 'ses_hist_1',
                 'traceloop.association.properties.user_id': 'usr_hist_1',
                 'gen_ai.usage.input_tokens': '25854',
                 'gen_ai.usage.output_tokens': '2447',
                 'gen_ai.usage.total_tokens': '28301'
               },
               MAP {'service.name': 'travel-agent'},
               '[]', 'OK', NULL
             ),
             (
               't_hist_1', 't_hist_1', 'sp_gen', 'sp_agent', 'fallback-app', 'ChatVertexAI', 'Client',
               TIMESTAMP_NS '2026-07-18 12:00:01', TIMESTAMP_NS '2026-07-18 12:00:04',
               MAP {
                 'openinference.span.kind': 'llm',
                 'traceloop.association.properties.session_id': 'ses_hist_1',
                 'llm.model_name': 'gemini-2.5-flash',
                 'llm.system': 'vertexai',
                 'llm.token_count.prompt': '25854',
                 'llm.token_count.completion': '2447',
                 'llm.token_count.total': '28301'
               },
               MAP {},
               '[]', 'ERROR', 'upstream rate limit'
             ),
             (
               't_hist_1', 't_hist_1', 'sp_body', 'sp_agent', 'fallback-app', 'softprobe.http.body.capture', 'Unspecified',
               TIMESTAMP_NS '2026-07-18 12:00:02', TIMESTAMP_NS '2026-07-18 12:00:02',
               MAP {'traceloop.association.properties.session_id': 'ses_hist_1'},
               MAP {},
               '[]', 'UNSET', NULL
             );",
        )
        .expect("insert historical rows");

        let from = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap();

        let detail_sql = compile_session_detail_sql("ses_hist_1", from, to).expect("detail sql");
        crate::sql::ensure_fact_scan_uses_timestamp_pruning(&conn, &detail_sql)
            .expect("detail pruning gate");
        struct DetailRow {
            span_id: String,
            span_type: String,
            service_name: Option<String>,
            span_kind: Option<String>,
            status_message: Option<String>,
            model_name: Option<String>,
            model_provider: Option<String>,
            session_span_count: i64,
            session_input_tokens: Option<i64>,
            session_output_tokens: Option<i64>,
            session_total_tokens: Option<i64>,
        }

        let mut stmt = conn.prepare(&detail_sql).expect("prepare detail");
        let rows: Vec<DetailRow> = stmt
            .query_map([], |row| {
                Ok(DetailRow {
                    span_id: row.get("span_id")?,
                    span_type: row.get("span_type")?,
                    service_name: row.get("service_name")?,
                    span_kind: row.get("span_kind")?,
                    status_message: row.get("status_message")?,
                    model_name: row.get("model_name")?,
                    model_provider: row.get("model_provider")?,
                    session_span_count: row.get("session_span_count")?,
                    session_input_tokens: row.get("session_input_tokens")?,
                    session_output_tokens: row.get("session_output_tokens")?,
                    session_total_tokens: row.get("session_total_tokens")?,
                })
            })
            .expect("query detail")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect detail");

        assert_eq!(
            rows.len(),
            3,
            "http_body stays in SessionDetail.spans for body coalescing"
        );
        let agent_row = rows
            .iter()
            .find(|r| r.span_id == "sp_agent")
            .expect("agent row");
        assert_eq!(agent_row.span_type, "agent");
        assert_eq!(agent_row.service_name.as_deref(), Some("travel-agent"));
        assert_eq!(agent_row.span_kind.as_deref(), Some("INTERNAL"));
        assert_eq!(
            agent_row.session_span_count, 2,
            "session_span_count excludes http_body"
        );
        assert_eq!(agent_row.session_input_tokens, Some(25854));
        assert_eq!(agent_row.session_output_tokens, Some(2447));
        assert_eq!(agent_row.session_total_tokens, Some(28301));

        let gen_row = rows
            .iter()
            .find(|r| r.span_id == "sp_gen")
            .expect("gen row");
        assert_eq!(gen_row.span_type, "generation");
        assert_eq!(gen_row.service_name.as_deref(), Some("fallback-app"));
        assert_eq!(gen_row.span_kind.as_deref(), Some("CLIENT"));
        assert_eq!(
            gen_row.status_message.as_deref(),
            Some("upstream rate limit")
        );
        assert_eq!(gen_row.model_name.as_deref(), Some("gemini-2.5-flash"));
        assert_eq!(gen_row.model_provider.as_deref(), Some("vertexai"));

        let body_row = rows
            .iter()
            .find(|r| r.span_id == "sp_body")
            .expect("body row");
        assert_eq!(body_row.span_type, "http_body");
        assert_eq!(
            body_row.span_kind, None,
            "Unspecified span_kind normalizes to NULL"
        );

        let search_req = SessionSearchRequest {
            from,
            to,
            user_id: Some("usr_hist_1".into()),
            agent_name: Some("concierge-agent".into()),
            model_name: Some("gemini-2.5-flash".into()),
            has_errors: Some(true),
            roots_only: false,
            order_by: SessionOrderBy::StartTime,
            order: SortDirection::Desc,
            limit: Some(10),
            cursor: None,
        };
        let search_sql = compile_session_search_sql(&search_req, 10).expect("session search sql");
        crate::sql::ensure_fact_scan_uses_timestamp_pruning(&conn, &search_sql)
            .expect("session search pruning gate");
        struct SummaryRow {
            session_id: String,
            observation_count: i64,
            input_tokens: Option<i64>,
            output_tokens: Option<i64>,
            total_tokens: Option<i64>,
            agent_name: Option<String>,
        }
        let summary_row: SummaryRow = conn
            .query_row(&search_sql, [], |row| {
                Ok(SummaryRow {
                    session_id: row.get("session_id")?,
                    observation_count: row.get("observation_count")?,
                    input_tokens: row.get("input_tokens")?,
                    output_tokens: row.get("output_tokens")?,
                    total_tokens: row.get("total_tokens")?,
                    agent_name: row.get("agent_name")?,
                })
            })
            .expect("session search row");
        assert_eq!(summary_row.session_id, "ses_hist_1");
        assert_eq!(summary_row.observation_count, 2);
        assert_eq!(summary_row.input_tokens, Some(25854));
        assert_eq!(summary_row.output_tokens, Some(2447));
        assert_eq!(summary_row.total_tokens, Some(28301));
        assert_eq!(summary_row.agent_name.as_deref(), Some("concierge-agent"));

        let trace_sql =
            compile_trace_summary_sql("t_hist_1", from, to, None).expect("trace summary sql");
        crate::sql::ensure_fact_scan_uses_timestamp_pruning(&conn, &trace_sql)
            .expect("trace summary pruning gate");
        let trace_tokens: (Option<i64>, Option<i64>, Option<i64>) = conn
            .query_row(&trace_sql, [], |row| {
                Ok((
                    row.get("input_tokens")?,
                    row.get("output_tokens")?,
                    row.get("total_tokens")?,
                ))
            })
            .expect("trace summary row");
        assert_eq!(trace_tokens, (Some(25854), Some(2447), Some(28301)));
    }

    #[test]
    fn usage_leaf_rollup_handles_nested_generations_reasoning_totals_and_parent_cycles() {
        let conn = open_duckdb_with_traces_and_scores();
        conn.execute_batch(
            "INSERT INTO traces (
               session_id, trace_id, span_id, parent_span_id, app_id, message_type,
               timestamp, observation_type, input_tokens, output_tokens, total_tokens
             ) VALUES
             ('ses_leaf', 'tr_1', 'outer_gen', NULL, 'app', 'ai.streamText',
              TIMESTAMP_NS '2026-07-18 12:00:00', 'generation', 100, 25, 140),
             ('ses_leaf', 'tr_1', 'inner_gen', 'outer_gen', 'app', 'ai.streamText.doStream',
              TIMESTAMP_NS '2026-07-18 12:00:01', 'generation', 100, 25, 140),
             ('ses_leaf', 'tr_2', 'gen_no_total', NULL, 'app', 'chat',
              TIMESTAMP_NS '2026-07-18 12:01:00', 'generation', 30, 20, NULL),
             ('ses_cycle', 'tr_cyc', 'cyc_a', 'cyc_b', 'app', 'agent',
              TIMESTAMP_NS '2026-07-18 12:02:00', 'agent', NULL, NULL, NULL),
             ('ses_cycle', 'tr_cyc', 'cyc_b', 'cyc_a', 'app', 'chat',
              TIMESTAMP_NS '2026-07-18 12:02:01', 'generation', 10, 5, 18);",
        )
        .expect("insert leaf and cycle spans");

        let from = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap();

        let leaf_sql = compile_session_detail_sql("ses_leaf", from, to).expect("leaf sql");
        let totals: (Option<i64>, Option<i64>, Option<i64>) = conn
            .query_row(&leaf_sql, [], |row| {
                Ok((
                    row.get("session_input_tokens")?,
                    row.get("session_output_tokens")?,
                    row.get("session_total_tokens")?,
                ))
            })
            .expect("leaf session detail");
        assert_eq!(totals, (Some(130), Some(45), Some(190)));

        let cyc_sql = compile_session_detail_sql("ses_cycle", from, to).expect("cycle sql");
        let cyc_totals: (Option<i64>, Option<i64>, Option<i64>) = conn
            .query_row(&cyc_sql, [], |row| {
                Ok((
                    row.get("session_input_tokens")?,
                    row.get("session_output_tokens")?,
                    row.get("session_total_tokens")?,
                ))
            })
            .expect("cycle session detail");
        assert_eq!(cyc_totals, (Some(10), Some(5), Some(18)));
    }
}
