//! LLM lake search request/response contracts (recipe inputs and list shapes).
//!
//! Owned here so `sql::llm` recipes do not import `api`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::models::Score;
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpanSearchRequest {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    #[serde(default)]
    pub span_types: Vec<String>,
    pub model_name: Option<String>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub trace_id: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpanSearchResponse {
    pub items: Vec<SpanSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpanSummary {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub session_id: Option<String>,
    pub name: String,
    pub span_type: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status_code: Option<String>,
    pub model_name: Option<String>,
    pub model_provider: Option<String>,
    pub agent_name: Option<String>,
    pub user_id: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanDetail {
    #[serde(flatten)]
    pub summary: SpanSummary,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub events: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scores: Vec<Score>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trace {
    pub trace_id: String,
    pub session_id: Option<String>,
    pub name: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub span_count: i64,
    pub error_count: i64,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetail {
    pub trace: Trace,
    pub spans: Vec<SpanDetail>,
    pub scores: Vec<Score>,
    pub next_span_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDetail {
    pub session_id: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub trace_count: i64,
    pub span_count: i64,
    #[serde(default)]
    pub user_ids: Vec<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub total_cost: Option<f64>,
    pub spans: Vec<SpanDetail>,
    pub scores: Vec<Score>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_search_request_serde_round_trip() {
        let json = r#"{
            "from": "2026-07-18T00:00:00Z",
            "to": "2026-07-19T00:00:00Z",
            "span_types": ["generation"],
            "limit": 10
        }"#;
        let req: SpanSearchRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(req.span_types, vec!["generation".to_string()]);
        assert_eq!(req.limit, Some(10));
        let again: SpanSearchRequest =
            serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap();
        assert_eq!(again, req);
    }
}
