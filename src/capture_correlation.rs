//! Capture span classification and correlation identity.
//!
//! This module deliberately contains no process-local join state. Runtime
//! correlation must be backed by durable, bounded storage before ingest calls
//! these helpers; an in-memory map would lose either arrival order on restart.

use crate::models::Span;
use anyhow::{bail, Result};

mod store;
pub(crate) use store::CaptureCorrelationStore;
mod job;
pub use job::start_capture_expiry_sweeper;

pub const CAPTURE_SOURCE_ATTRIBUTE: &str = "sp.capture.source";
pub const OBSERVATION_TYPE_ATTRIBUTE: &str = "sp.observation.type";
pub const CAPTURE_ID_ATTRIBUTE: &str = "sp.capture.id";

fn capture_attribute<'a>(span: &'a Span, key: &str) -> Option<&'a String> {
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
    pub capture_id: String,
    pub trace_id: String,
}

impl CaptureTraceKey {
    /// The capture id must come from the verified internal assertion, never
    /// from an OTLP resource or span attribute.
    pub fn new(
        workspace_id: impl Into<String>,
        trusted_capture_id: impl Into<String>,
        trace_id: impl Into<String>,
    ) -> Result<Self> {
        let workspace_id = workspace_id.into();
        let capture_id = trusted_capture_id.into();
        let trace_id = trace_id.into();
        if workspace_id.trim().is_empty()
            || capture_id.trim().is_empty()
            || trace_id.trim().is_empty()
        {
            bail!("capture correlation requires workspace, capture, and trace identities");
        }
        Ok(Self {
            workspace_id,
            capture_id,
            trace_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureObservation {
    /// Browser session mappings label backend spans that share this trace id.
    BrowserMapping {
        key: CaptureTraceKey,
        session_id: String,
    },
    /// OBI spans are staged durably until a browser mapping is available.
    BackendSpan { key: CaptureTraceKey },
    /// Recording observations carry session data but do not map request traces.
    Recording,
}

/// Interpret only the explicit capture markers. Ordinary telemetry remains on
/// the existing ingest path. `trusted_capture_id` is supplied by verified auth.
pub fn classify_capture_span(
    span: &Span,
    workspace_id: &str,
    trusted_capture_id: Option<&str>,
) -> Result<Option<CaptureObservation>> {
    let recording = capture_attribute(span, OBSERVATION_TYPE_ATTRIBUTE)
        .is_some_and(|value| value == "recording");
    let source = capture_attribute(span, CAPTURE_SOURCE_ATTRIBUTE);
    let span_capture_id = capture_attribute(span, CAPTURE_ID_ATTRIBUTE);
    if source.is_none() && span_capture_id.is_none() {
        return Ok(None);
    }

    let capture_id = trusted_capture_id
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("capture span requires a trusted capture assertion"))?;
    if span_capture_id.is_some_and(|value| value.trim() != capture_id.trim()) {
        bail!("span capture id does not match the trusted capture assertion");
    }
    if recording {
        if source.map(String::as_str) != Some("browser") {
            bail!("capture recording requires browser source");
        }
        return Ok(Some(CaptureObservation::Recording));
    }
    let Some(source) = source else {
        bail!("capture span requires an explicit source or recording marker");
    };
    let key = CaptureTraceKey::new(workspace_id, capture_id, span.trace_id.clone())?;

    match source.as_str() {
        "browser" => {
            let session_id = span_session_id(span)
                .map(String::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| anyhow::anyhow!("browser capture span requires session.id"))?;
            Ok(Some(CaptureObservation::BrowserMapping {
                key,
                session_id: session_id.to_owned(),
            }))
        }
        "obi" => {
            if span_session_id(span).is_some()
                || span.resource_attributes.contains_key("session.id")
                || span.resource_attributes.contains_key("sp.session.id")
            {
                bail!("OBI capture spans cannot supply a browser session mapping");
            }
            Ok(Some(CaptureObservation::BackendSpan { key }))
        }
        other => bail!("unsupported capture span source: {other}"),
    }
}

/// Validate capture classification before the generic session-id fallback can
/// reach persistence. The durable join store stages OBI spans and assigns a
/// browser session before writing them to the lake.
pub fn validate_capture_batch(
    spans: &[Span],
    workspace_id: &str,
    trusted_capture_id: Option<&str>,
) -> Result<()> {
    for span in spans {
        match classify_capture_span(span, workspace_id, trusted_capture_id)? {
            None if trusted_capture_id.is_some() => {
                bail!("capture ingest contains a span without a capture classification")
            }
            None => {}
            Some(CaptureObservation::Recording) => {
                let has_session =
                    span_session_id(span).is_some_and(|session_id| !session_id.trim().is_empty());
                if !has_session {
                    bail!("capture recording requires a real session id before persistence");
                }
            }
            Some(CaptureObservation::BrowserMapping { .. }) => {}
            Some(CaptureObservation::BackendSpan { .. }) => {}
        }
    }
    Ok(())
}

/// Apply a browser mapping immediately before a correlated OBI span is written.
/// Callers must reject the span when no durable mapping is available.
pub fn assign_session_id(span: &mut Span, session_id: &str) -> Result<()> {
    let session_id = session_id.trim();
    if session_id.is_empty() {
        bail!("capture session mapping cannot be empty");
    }
    if capture_attribute(span, CAPTURE_SOURCE_ATTRIBUTE).map(String::as_str) != Some("obi") {
        bail!("session mappings may only be assigned to OBI capture spans");
    }
    if span.attributes.contains_key("sp.session.id") || span.attributes.contains_key("session.id") {
        bail!("OBI capture spans must not supply a browser session mapping");
    }
    span.session_id = session_id.to_owned();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn span(source: &str, trace_id: &str) -> Span {
        let mut attributes = HashMap::new();
        attributes.insert(CAPTURE_SOURCE_ATTRIBUTE.into(), source.into());
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
            attributes,
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
    fn correlation_key_isolated_by_workspace_capture_and_trace() {
        let key = CaptureTraceKey::new("workspace-a", "capture-a", "trace-a").unwrap();
        assert_ne!(
            key,
            CaptureTraceKey::new("workspace-b", "capture-a", "trace-a").unwrap()
        );
        assert_ne!(
            key,
            CaptureTraceKey::new("workspace-a", "capture-b", "trace-a").unwrap()
        );
        assert_ne!(
            key,
            CaptureTraceKey::new("workspace-a", "capture-a", "trace-b").unwrap()
        );
    }

    #[test]
    fn browser_mapping_uses_only_trusted_capture_scope_and_session_id() {
        let mut browser = span("browser", "trace-a");
        browser
            .attributes
            .insert("session.id".into(), "session-a".into());

        assert_eq!(
            classify_capture_span(&browser, "workspace-a", Some("trusted-capture-a")).unwrap(),
            Some(CaptureObservation::BrowserMapping {
                key: CaptureTraceKey::new("workspace-a", "trusted-capture-a", "trace-a").unwrap(),
                session_id: "session-a".into(),
            })
        );
        assert!(classify_capture_span(&browser, "workspace-a", None).is_err());
        browser
            .attributes
            .insert("sp.capture.id".into(), "untrusted-span-capture".into());
        assert!(classify_capture_span(&browser, "workspace-a", Some("trusted-capture-a")).is_err());
    }

    #[test]
    fn obi_span_needs_a_mapping_assignment_before_persistence() {
        let mut obi = span("obi", "trace-a");
        assert!(matches!(
            classify_capture_span(&obi, "workspace-a", Some("capture-a")).unwrap(),
            Some(CaptureObservation::BackendSpan { .. })
        ));
        assert_eq!(obi.session_id, "trace-a"); // model's generic fallback is not accepted as correlation.
        assign_session_id(&mut obi, "session-a").unwrap();
        assert_eq!(obi.session_id, "session-a");
        assert!(assign_session_id(&mut obi, "").is_err());
    }

    #[test]
    fn obi_session_mapping_accepts_resource_source_marker() {
        let mut obi = span("obi", "trace-a");
        obi.attributes.remove(CAPTURE_SOURCE_ATTRIBUTE);
        obi.resource_attributes
            .insert(CAPTURE_SOURCE_ATTRIBUTE.into(), "obi".into());

        assign_session_id(&mut obi, "session-a").unwrap();
        assert_eq!(obi.session_id, "session-a");
    }

    #[test]
    fn recording_observations_do_not_create_trace_mappings() {
        let mut recording = span("browser", "trace-a");
        recording
            .attributes
            .insert(OBSERVATION_TYPE_ATTRIBUTE.into(), "recording".into());
        assert_eq!(
            classify_capture_span(&recording, "workspace-a", Some("capture-a")).unwrap(),
            Some(CaptureObservation::Recording)
        );
        assert!(classify_capture_span(&recording, "workspace-a", None).is_err());
    }

    #[test]
    fn obi_span_cannot_bypass_correlation_by_claiming_recording_type() {
        let mut obi = span("obi", "trace-a");
        obi.attributes
            .insert(OBSERVATION_TYPE_ATTRIBUTE.into(), "recording".into());
        obi.attributes
            .insert("sp.session.id".into(), "forged-session".into());

        assert!(validate_capture_batch(&[obi], "workspace-a", Some("capture-a")).is_err());
    }

    #[test]
    fn recording_requires_a_span_level_session_id() {
        let mut recording = span("browser", "trace-a");
        recording
            .attributes
            .insert(OBSERVATION_TYPE_ATTRIBUTE.into(), "recording".into());
        recording
            .resource_attributes
            .insert("session.id".into(), "resource-session".into());

        assert!(validate_capture_batch(&[recording], "workspace-a", Some("capture-a")).is_err());
    }

    #[test]
    fn legacy_recording_without_capture_markers_keeps_generic_ingest_behavior() {
        let mut recording = span("browser", "trace-a");
        recording.attributes.remove(CAPTURE_SOURCE_ATTRIBUTE);
        recording
            .attributes
            .insert(OBSERVATION_TYPE_ATTRIBUTE.into(), "recording".into());
        recording
            .attributes
            .insert("sp.session.id".into(), "session-a".into());

        assert_eq!(
            classify_capture_span(&recording, "workspace-a", None).unwrap(),
            None
        );
        assert!(validate_capture_batch(&[recording], "workspace-a", None).is_ok());
    }

    #[test]
    fn capture_batch_accepts_unmatched_obi_for_durable_staging() {
        let obi = span("obi", "trace-a");
        validate_capture_batch(&[obi], "workspace-a", Some("capture-a"))
            .expect("unmatched OBI is accepted by validation for durable staging");
    }

    #[test]
    fn unmatched_obi_batch_can_be_accepted_for_durable_staging() {
        let obi = span("obi", "trace-a");
        assert!(validate_capture_batch(&[obi], "workspace-a", Some("capture-a")).is_ok());
    }

    #[test]
    fn capture_batch_rejects_untrusted_or_mismatched_profile_ids() {
        let mut browser = span("browser", "trace-a");
        browser
            .attributes
            .insert("sp.session.id".into(), "session-a".into());
        browser
            .attributes
            .insert(CAPTURE_ID_ATTRIBUTE.into(), "capture-other".into());
        assert!(validate_capture_batch(&[browser.clone()], "workspace-a", None).is_err());
        assert!(validate_capture_batch(&[browser], "workspace-a", Some("capture-a")).is_err());
    }

    #[test]
    fn obi_capture_markers_can_come_from_resource_attributes() {
        let mut obi = span("obi", "trace-a");
        obi.attributes.remove(CAPTURE_SOURCE_ATTRIBUTE);
        obi.resource_attributes
            .insert(CAPTURE_SOURCE_ATTRIBUTE.into(), "obi".into());
        obi.resource_attributes
            .insert(CAPTURE_ID_ATTRIBUTE.into(), "capture-a".into());
        validate_capture_batch(&[obi], "workspace-a", Some("capture-a"))
            .expect("resource-marked OBI spans are eligible for durable staging");
    }

    #[test]
    fn obi_capture_span_cannot_supply_its_own_session_mapping() {
        let mut obi = span("obi", "trace-a");
        obi.attributes
            .insert("session.id".into(), "claimed-session".into());
        assert!(validate_capture_batch(&[obi], "workspace-a", Some("capture-a")).is_err());
    }
}
