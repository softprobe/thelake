//! Session-list wire contract (Postgres `session_summary` list path).
//!
//! Owned here so SQL recipes and list orchestration do not import `api`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::sql::paging::encode_cursor;

/// How a session list should be ordered.
///
/// Ordering happens over the whole time window. Doing it client-side only ever
/// sorts whatever page happened to be loaded, which is the wrong answer to
/// "show me the worst sessions today".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionOrderBy {
    #[default]
    StartTime,
    ErrorCount,
    Duration,
    TotalTokens,
    TotalCost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    #[default]
    Desc,
}

impl SortDirection {
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SessionSearchRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    /// Keep only sessions containing at least one ERROR span.
    #[serde(default)]
    pub has_errors: Option<bool>,
    pub user_id: Option<String>,
    pub model_name: Option<String>,
    /// Match session-level `agent_name` (persisted column, `sp.agent.name`, else agent span name).
    pub agent_name: Option<String>,
    /// When true (default), hide legacy nested-only OpenCode child sessions.
    #[serde(default = "default_true")]
    pub roots_only: bool,
    #[serde(default)]
    pub order_by: SessionOrderBy,
    #[serde(default)]
    pub order: SortDirection,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionSummary {
    pub session_id: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub trace_count: i64,
    pub span_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub agent_name: Option<String>,
    pub user_ids: Vec<String>,
    pub models: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionSearchResponse {
    pub items: Vec<SessionSummary>,
    pub next_cursor: Option<String>,
    /// Cursor paging is only defined for `start_time` ordering; any other
    /// ordering returns a single ranked page. Stated explicitly so a client
    /// cannot mistake "no cursor" for "no more data".
    pub cursor_supported: bool,
}

/// Truncate `items` to `limit` and return an opaque cursor when the page was
/// actually cut short.
pub(crate) fn next_cursor_from_sessions(
    items: &mut Vec<SessionSummary>,
    limit: usize,
) -> Option<String> {
    if items.len() <= limit {
        return None;
    }
    items.truncate(limit);
    items
        .last()
        .map(|item| encode_cursor(item.start_time, &item.session_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn session_search_request_serde_round_trip_defaults() {
        let json = r#"{
            "from": "2026-07-18T00:00:00Z",
            "to": "2026-07-19T00:00:00Z"
        }"#;
        let req: SessionSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert!(req.roots_only);
        assert_eq!(req.order_by, SessionOrderBy::StartTime);
        assert_eq!(req.order, SortDirection::Desc);
        let again: SessionSearchRequest =
            serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        assert_eq!(again, req);
    }

    #[test]
    fn next_cursor_from_sessions_truncates_and_encodes() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 18, 1, 0, 0).unwrap();
        let mut items = vec![
            SessionSummary {
                session_id: "s0".into(),
                start_time: t0,
                end_time: None,
                trace_count: 1,
                span_count: 1,
                error_count: 0,
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                total_cost: None,
                agent_name: None,
                user_ids: vec![],
                models: vec![],
            },
            SessionSummary {
                session_id: "s1".into(),
                start_time: t1,
                end_time: None,
                trace_count: 1,
                span_count: 1,
                error_count: 0,
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                total_cost: None,
                agent_name: None,
                user_ids: vec![],
                models: vec![],
            },
        ];
        let cursor = next_cursor_from_sessions(&mut items, 1).expect("cursor");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].session_id, "s0");
        let decoded = crate::sql::paging::decode_cursor(&cursor).expect("decode");
        assert_eq!(decoded.id, "s0");
        assert_eq!(decoded.t, t0);
    }
}
