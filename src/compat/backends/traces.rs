use crate::compat::errors::CompatError;
use crate::compat::tempo::traceql::TraceSelector;
use crate::compat::tenant::TenantContext;
use crate::sql::tempo::TraceScanParams;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceAttribute {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEvent {
    pub name: String,
    pub timestamp_unix_nano: i64,
    pub attributes: Vec<TraceAttribute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: Option<String>,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: Option<i64>,
    pub attributes: Vec<TraceAttribute>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
    pub events: Vec<TraceEvent>,
    pub service_name: Option<String>,
    pub resource_attributes: Vec<TraceAttribute>,
    pub instrumentation_scope: Option<serde_json::Value>,
    pub links: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceData {
    pub spans: Vec<TraceSpan>,
}

/// Return span attributes that are visible on the Tempo protocol surface.
///
/// `service_name` is retained as a typed field for storage and query planning,
/// but when it is duplicated in the attribute bag it is an internal ingestion
/// representation rather than a user-visible Tempo attribute.
pub(crate) fn visible_span_attributes(span: &TraceSpan) -> Vec<TraceAttribute> {
    span.attributes
        .iter()
        .filter(|attribute| {
            !(attribute.key == "service_name"
                && span.service_name.as_deref() == Some(attribute.value.as_str()))
        })
        .cloned()
        .collect()
}

pub(crate) fn persisted_status_code_numeric_value(value: &str) -> Option<i64> {
    crate::compat::tempo::traceql::canonical_status_code(value)
}

#[derive(Debug, Clone, PartialEq)]
pub struct TraceSearchHit {
    pub trace_id: String,
    pub root_service_name: Option<String>,
    pub root_trace_name: Option<String>,
    pub start_time_unix_nano: i64,
    pub duration_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSearchRequest {
    pub tags: BTreeMap<String, String>,
    pub selector: Option<TraceSelector>,
    pub min_duration_ns: Option<i64>,
    pub max_duration_ns: Option<i64>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: usize,
}

impl TraceSearchRequest {
    /// Map protocol request → lake scan params for [`crate::sql::tempo`].
    pub fn scan_params(&self) -> TraceScanParams<'_> {
        TraceScanParams {
            tags: &self.tags,
            selector: self.selector.as_ref(),
            min_duration_ns: self.min_duration_ns,
            max_duration_ns: self.max_duration_ns,
            start_ns: self.start_ns,
            end_ns: self.end_ns,
            limit: self.limit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceLookupBounds {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

#[async_trait]
pub trait TraceQueryBackend: Send + Sync {
    async fn get_trace(
        &self,
        ctx: &TenantContext,
        trace_id: &str,
        bounds: TraceLookupBounds,
    ) -> Result<Option<TraceData>, CompatError>;

    async fn search(
        &self,
        ctx: &TenantContext,
        request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError>;

    async fn search_tags(&self, ctx: &TenantContext) -> Result<Vec<String>, CompatError>;

    async fn search_tag_values(
        &self,
        ctx: &TenantContext,
        tag: &str,
    ) -> Result<Vec<String>, CompatError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnsupportedTraceBackend;

#[async_trait]
impl TraceQueryBackend for UnsupportedTraceBackend {
    async fn get_trace(
        &self,
        _ctx: &TenantContext,
        _trace_id: &str,
        _bounds: TraceLookupBounds,
    ) -> Result<Option<TraceData>, CompatError> {
        Err(CompatError::unsupported("trace_get"))
    }

    async fn search(
        &self,
        _ctx: &TenantContext,
        _request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError> {
        Err(CompatError::unsupported("trace_search"))
    }

    async fn search_tags(&self, _ctx: &TenantContext) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("trace_search_tags"))
    }

    async fn search_tag_values(
        &self,
        _ctx: &TenantContext,
        _tag: &str,
    ) -> Result<Vec<String>, CompatError> {
        Err(CompatError::unsupported("trace_search_tag_values"))
    }
}
