//! Shared Span fixtures for session_summary tests (DRY).

use crate::models::Span;
use chrono::{TimeZone, Utc};
use std::collections::HashMap;

pub fn span_at(session_id: &str, secs: i64) -> Span {
    Span {
        session_id: session_id.to_string(),
        trace_id: "t".into(),
        span_id: format!("sp-{secs}"),
        parent_span_id: None,
        app_id: "app".into(),
        organization_id: None,
        tenant_id: None,
        agent_id: None,
        agent_name: None,
        message_type: "msg".into(),
        span_kind: None,
        timestamp: Utc.timestamp_opt(secs, 0).unwrap(),
        end_timestamp: None,
        attributes: HashMap::new(),
        resource_attributes: HashMap::new(),
        events: Vec::new(),
        status_code: None,
        status_message: None,
        http_request_method: None,
        http_request_path: None,
        http_request_headers: None,
        http_request_body: None,
        http_response_status_code: None,
        http_response_headers: None,
        http_response_body: None,
    }
}
