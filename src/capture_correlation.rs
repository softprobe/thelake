//! Agent trace and recording classification and correlation identity.
//!
//! This module deliberately contains no process-local join state. Runtime
//! correlation must be backed by durable, bounded storage before ingest calls
//! these helpers; an in-memory map would lose either arrival order on restart.

use crate::models::{attr_keys::sp::OBSERVATION_TYPE, Span};
use anyhow::{bail, Result};

mod store;
pub(crate) use store::CaptureCorrelationStore;
mod job;
pub use job::start_capture_expiry_sweeper;

const RECORDING_OBSERVATION_TYPE: &str = "recording";

fn observation_attribute<'a>(span: &'a Span, key: &str) -> Option<&'a String> {
    span.attributes
        .get(key)
        .or_else(|| span.resource_attributes.get(key))
}

fn span_session_id(span: &Span) -> Option<&String> {
    span.attributes
        .get("sp.session.id")
        .or_else(|| span.attributes.get("sp_session_id"))
        .or_else(|| span.attributes.get("session.id"))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CaptureTraceKey {
    pub workspace_id: String,
    pub agent_id: String,
    pub trace_id: String,
}

impl CaptureTraceKey {
    /// The Agent id must come from the verified internal assertion, never from
    /// an OTLP resource or span attribute.
    pub fn new(
        workspace_id: impl Into<String>,
        trusted_agent_id: impl Into<String>,
        trace_id: impl Into<String>,
    ) -> Result<Self> {
        let workspace_id = workspace_id.into();
        let agent_id = trusted_agent_id.into();
        let trace_id = trace_id.into();
        if workspace_id.trim().is_empty()
            || agent_id.trim().is_empty()
            || trace_id.trim().is_empty()
        {
            bail!("capture correlation requires workspace, Agent, and trace identities");
        }
        Ok(Self {
            workspace_id,
            agent_id,
            trace_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureObservation {
    /// Any trace span with a Session ID maps other spans with the same trace.
    Trace {
        key: CaptureTraceKey,
        session_id: Option<String>,
    },
    /// RRWeb recordings are persisted directly and do not map trace spans.
    Recording,
}

/// Classify the two Agent-authenticated telemetry shapes: trace spans and
/// rrweb recordings. `trusted_agent_id` comes from verified Agent auth.
pub fn classify_capture_span(
    span: &Span,
    workspace_id: &str,
    trusted_agent_id: Option<&str>,
) -> Result<Option<CaptureObservation>> {
    let recording = observation_attribute(span, OBSERVATION_TYPE)
        .is_some_and(|value| value == RECORDING_OBSERVATION_TYPE);
    if recording {
        trusted_agent_id
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("recording requires a trusted Agent assertion"))?;
        span_session_id(span)
            .map(String::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow::anyhow!("recording requires a real session.id"))?;
        return Ok(Some(CaptureObservation::Recording));
    }
    let Some(agent_id) = trusted_agent_id.filter(|id| !id.trim().is_empty()) else {
        return Ok(None);
    };
    let key = CaptureTraceKey::new(workspace_id, agent_id, span.trace_id.clone())?;
    let session_id = span_session_id(span)
        .map(String::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned);
    Ok(Some(CaptureObservation::Trace { key, session_id }))
}

/// Validate Agent trace and recording spans before the generic session-id
/// fallback can reach persistence. The durable join store stages trace spans
/// until a span in the same trace supplies a Session ID.
pub fn validate_capture_batch(
    spans: &[Span],
    workspace_id: &str,
    trusted_agent_id: Option<&str>,
) -> Result<()> {
    for span in spans {
        classify_capture_span(span, workspace_id, trusted_agent_id)?;
    }
    Ok(())
}

pub fn uses_capture_correlation(spans: &[Span], trusted_agent_id: Option<&str>) -> bool {
    trusted_agent_id.is_some_and(|id| !id.trim().is_empty())
        || spans.iter().any(|span| {
            observation_attribute(span, OBSERVATION_TYPE)
                .is_some_and(|value| value == RECORDING_OBSERVATION_TYPE)
        })
}

/// Assign the correlated Session ID immediately before a trace span is written.
/// Callers must reject the span when no durable mapping is available.
pub fn assign_session_id(span: &mut Span, session_id: &str) -> Result<()> {
    let session_id = session_id.trim();
    if session_id.is_empty() {
        bail!("capture session mapping cannot be empty");
    }
    if span_session_id(span).is_some_and(|id| !id.trim().is_empty()) {
        bail!("trace span already has a session.id");
    }
    span.session_id = session_id.to_owned();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn span(trace_id: &str) -> Span {
        Span {
            session_id: trace_id.into(),
            trace_id: trace_id.into(),
            span_id: "span".into(),
            parent_span_id: None,
            app_id: "test-app".into(),
            organization_id: None,
            workspace_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "request".into(),
            span_kind: None,
            timestamp: chrono::Utc::now(),
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

    #[test]
    fn correlation_key_isolated_by_workspace_agent_and_trace() {
        let key = CaptureTraceKey::new("workspace-a", "agent-a", "trace-a").unwrap();
        assert_ne!(
            key,
            CaptureTraceKey::new("workspace-b", "agent-a", "trace-a").unwrap()
        );
        assert_ne!(
            key,
            CaptureTraceKey::new("workspace-a", "agent-b", "trace-a").unwrap()
        );
        assert_ne!(
            key,
            CaptureTraceKey::new("workspace-a", "agent-a", "trace-b").unwrap()
        );
    }

    #[test]
    fn trace_session_id_supplies_mapping_with_trusted_agent_scope() {
        let mut mapped = span("trace-a");
        mapped
            .attributes
            .insert("session.id".into(), "session-a".into());

        assert_eq!(
            classify_capture_span(&mapped, "workspace-a", Some("agent-a")).unwrap(),
            Some(CaptureObservation::Trace {
                key: CaptureTraceKey::new("workspace-a", "agent-a", "trace-a").unwrap(),
                session_id: Some("session-a".into()),
            })
        );
        assert!(classify_capture_span(&mapped, "workspace-a", None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn trace_without_session_id_waits_for_mapping_before_persistence() {
        let mut trace = span("trace-a");
        assert!(matches!(
            classify_capture_span(&trace, "workspace-a", Some("agent-a")).unwrap(),
            Some(CaptureObservation::Trace {
                session_id: None,
                ..
            })
        ));
        assert_eq!(trace.session_id, "trace-a");
        assign_session_id(&mut trace, "session-a").unwrap();
        assert_eq!(trace.session_id, "session-a");
        assert!(assign_session_id(&mut trace, "").is_err());
    }

    #[test]
    fn recording_type_identifies_rrweb_and_requires_session_and_agent() {
        let mut recording = span("trace-a");
        recording
            .attributes
            .insert(OBSERVATION_TYPE.into(), "recording".into());
        recording
            .attributes
            .insert("sp.session.id".into(), "session-a".into());
        assert_eq!(
            classify_capture_span(&recording, "workspace-a", Some("agent-a")).unwrap(),
            Some(CaptureObservation::Recording)
        );
        assert!(classify_capture_span(&recording, "workspace-a", None).is_err());
        recording.attributes.remove("sp.session.id");
        assert!(classify_capture_span(&recording, "workspace-a", Some("agent-a")).is_err());
    }

    #[test]
    fn recording_requires_a_span_level_session_id() {
        let mut recording = span("trace-a");
        recording
            .attributes
            .insert(OBSERVATION_TYPE.into(), "recording".into());
        recording
            .resource_attributes
            .insert("session.id".into(), "resource-session".into());

        assert!(validate_capture_batch(&[recording], "workspace-a", Some("agent-a")).is_err());
    }

    #[test]
    fn all_agent_trace_spans_use_correlation_without_producer_markers() {
        let ordinary = span("trace-a");
        assert_eq!(
            classify_capture_span(&ordinary, "workspace-a", Some("agent-a")).unwrap(),
            Some(CaptureObservation::Trace {
                key: CaptureTraceKey::new("workspace-a", "agent-a", "trace-a").unwrap(),
                session_id: None,
            })
        );
        assert!(classify_capture_span(&ordinary, "workspace-a", None)
            .unwrap()
            .is_none());
        assert!(!uses_capture_correlation(
            std::slice::from_ref(&ordinary),
            None
        ));
        assert!(uses_capture_correlation(&[ordinary], Some("agent-a")));
    }

    #[test]
    fn non_recording_observation_types_are_trace_spans() {
        let mut trace = span("trace-a");
        trace
            .attributes
            .insert(OBSERVATION_TYPE.into(), "generation".into());
        assert!(matches!(
            classify_capture_span(&trace, "workspace-a", Some("agent-a")).unwrap(),
            Some(CaptureObservation::Trace {
                session_id: None,
                ..
            })
        ));
    }
}
