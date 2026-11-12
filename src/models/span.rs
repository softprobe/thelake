use anyhow::Result;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Internal JSON carriers used only while the canonical span model is persisted.
/// They keep OTLP scope/link metadata out of the user attribute namespace on read.
pub const INSTRUMENTATION_SCOPE_ATTRIBUTE: &str = "__softprobe.instrumentation_scope";
pub const LINKS_ATTRIBUTE: &str = "__softprobe.links";
/// Namespace prefix reserved for internal carriers; client telemetry must not use it.
pub const RESERVED_ATTRIBUTE_PREFIX: &str = "__softprobe.";

pub fn encode_instrumentation_scope(
    scope: &opentelemetry_proto::tonic::common::v1::InstrumentationScope,
) -> String {
    let attributes = crate::models::key_values_to_map(&scope.attributes)
        .into_iter()
        .map(|(key, value)| {
            serde_json::json!({
                "key": key,
                "value": {"stringValue": value},
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "name": scope.name,
        "version": scope.version,
        "attributes": attributes,
    })
    .to_string()
}

pub fn encode_links(links: &[opentelemetry_proto::tonic::trace::v1::span::Link]) -> String {
    serde_json::json!(links
        .iter()
        .map(|link| {
            let attributes = crate::models::key_values_to_map(&link.attributes)
                .into_iter()
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": key,
                        "value": {"stringValue": value},
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "traceId": hex::encode(&link.trace_id),
                "spanId": hex::encode(&link.span_id),
                "traceState": link.trace_state,
                "attributes": attributes,
                "flags": link.flags,
            })
        })
        .collect::<Vec<_>>())
    .to_string()
}

/// Span domain model - unified representation across all layers
/// Used for: OTLP ingestion → DuckLake storage → query results → JSON responses
///
/// This struct matches the telemetry schema in `src/storage/schema/tables.rs`
/// (legacy path; Arrow/DuckLake column order).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Span {
    // Field 1: session_id (REQUIRED)
    // Extracted from sp.session.id attribute or defaults to trace_id
    pub session_id: String,

    // Field 2-4: Primary identifiers
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,

    // Field 5-7: Application context
    pub app_id: String,
    pub organization_id: Option<String>,
    pub workspace_id: Option<String>,
    /// Softprobe agent id from assertion auth (not client OTLP).
    pub agent_id: Option<String>,
    /// Softprobe agent display name from assertion auth (not client OTLP).
    pub agent_name: Option<String>,

    // Field 8-11: Span metadata
    pub message_type: String,
    pub span_kind: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub end_timestamp: Option<chrono::DateTime<chrono::Utc>>,

    // Field 12: Attributes MAP<STRING, STRING>
    // Includes user-provided sp.* business attributes for search
    pub attributes: HashMap<String, String>,

    // Resource attributes retained for Tempo resource projection and promotion extraction.
    pub resource_attributes: HashMap<String, String>,

    // Field 13: Events ARRAY<STRUCT<name, timestamp, attributes>>
    // Contains http.request and http.response events with full bodies
    pub events: Vec<SpanEvent>,

    // Field 14-15: Status
    pub status_code: Option<String>,
    pub status_message: Option<String>,

    // Field 25-31: HTTP data (extracted from span events)
    // These are populated by extract_http_data_from_events() method
    // Stored separately from events for columnar I/O efficiency (per ADR-003)
    pub http_request_method: Option<String>,
    pub http_request_path: Option<String>,
    pub http_request_headers: Option<String>,
    pub http_request_body: Option<String>,
    pub http_response_status_code: Option<i32>,
    pub http_response_headers: Option<String>,
    pub http_response_body: Option<String>,
    // Partition day is derived from `timestamp` at write (year/month/day hive keys).
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub attributes: HashMap<String, String>,
}

impl Span {
    pub fn partition_key(&self) -> chrono::NaiveDate {
        crate::models::partition_day_from_event_time(self.timestamp)
    }

    pub fn grouping_key(&self) -> String {
        self.session_id.clone()
    }

    pub fn compare_for_sort(&self, other: &Self) -> Ordering {
        self.session_id
            .cmp(&other.session_id)
            .then_with(|| self.trace_id.cmp(&other.trace_id))
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }

    /// Convert a batch of Spans to Arrow RecordBatch for DuckLake storage
    ///
    /// This is a batch operation delegated to the arrow module since RecordBatch
    /// creation requires schema context and columnar array building
    pub fn to_record_batch(
        spans: &[Span],
        schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        crate::storage::schema::arrow::spans_to_record_batch(spans, schema)
    }

    /// Split by UTC calendar day of `timestamp` before building Arrow batches.
    pub fn to_record_batches_by_date(
        spans: Vec<Span>,
        schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
        crate::storage::schema::arrow::spans_to_record_batches_by_date(spans, schema)
    }

    /// Create a Span from an OTLP span and resource attributes
    pub fn from_otlp(
        otlp_span: opentelemetry_proto::tonic::trace::v1::Span,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<Self> {
        // Reject timestamps outside the signed-nanosecond range so they can
        // never silently persist as epoch/null downstream.
        for label in [
            ("start_time_unix_nano", otlp_span.start_time_unix_nano),
            ("end_time_unix_nano", otlp_span.end_time_unix_nano),
        ] {
            anyhow::ensure!(
                label.1 <= i64::MAX as u64,
                "span {} exceeds signed nanosecond range ({})",
                label.0,
                label.1
            );
        }
        for event in &otlp_span.events {
            anyhow::ensure!(
                event.time_unix_nano <= i64::MAX as u64,
                "span event time exceeds signed nanosecond range ({})",
                event.time_unix_nano
            );
        }
        let mut attributes = crate::models::key_values_to_map(&otlp_span.attributes);
        let canonical =
            canonicalize_multi_standard_llm_attributes(&otlp_span.name, &mut attributes);

        // Extract events
        let events = otlp_span
            .events
            .iter()
            .map(|event| {
                let event_timestamp = if event.time_unix_nano > 0 {
                    chrono::DateTime::from_timestamp(
                        (event.time_unix_nano / 1_000_000_000) as i64,
                        (event.time_unix_nano % 1_000_000_000) as u32,
                    )
                    .unwrap_or_else(chrono::Utc::now)
                } else {
                    chrono::Utc::now()
                };

                SpanEvent {
                    name: event.name.clone(),
                    timestamp: event_timestamp,
                    attributes: crate::models::key_values_to_map(&event.attributes),
                }
            })
            .collect();

        // Convert timestamps
        let timestamp = if otlp_span.start_time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (otlp_span.start_time_unix_nano / 1_000_000_000) as i64,
                (otlp_span.start_time_unix_nano % 1_000_000_000) as u32,
            )
            .unwrap_or_else(chrono::Utc::now)
        } else {
            chrono::Utc::now()
        };

        let end_timestamp = if otlp_span.end_time_unix_nano > 0 {
            Some(
                chrono::DateTime::from_timestamp(
                    (otlp_span.end_time_unix_nano / 1_000_000_000) as i64,
                    (otlp_span.end_time_unix_nano % 1_000_000_000) as u32,
                )
                .unwrap_or_else(chrono::Utc::now),
            )
        } else {
            None
        };

        // Explicit Softprobe app identity before generic OTEL `service.name`.
        // Resource `sp.app.id` / `sp_app_id` (backend), then span-wire `sp_app_id` /
        // `sp.app.id` (agent), then `service.name` as last-resort fallback.
        let app_id = resource_attributes
            .get("sp.app.id")
            .or_else(|| resource_attributes.get("sp_app_id"))
            .or_else(|| attributes.get("sp_app_id"))
            .or_else(|| attributes.get("sp.app.id"))
            .or_else(|| resource_attributes.get("service.name"))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let trace_id = hex::encode(&otlp_span.trace_id);

        let session_id = canonical.session_id.unwrap_or_else(|| trace_id.clone());

        let mut span = Self {
            session_id,
            trace_id,
            span_id: hex::encode(&otlp_span.span_id),
            parent_span_id: if otlp_span.parent_span_id.is_empty() {
                None
            } else {
                Some(hex::encode(&otlp_span.parent_span_id))
            },
            app_id,
            organization_id: resource_attributes.get("sp.organization.id").cloned(),
            workspace_id: resource_attributes.get("sp.tenant.id").cloned(),
            agent_id: None,
            agent_name: canonical.agent_name,
            message_type: otlp_span.name.clone(),
            span_kind: Some(format!("{:?}", otlp_span.kind())),
            timestamp,
            end_timestamp,
            attributes,
            resource_attributes: resource_attributes.clone(),
            events,
            // HTTP fields will be populated by extract_http_data_from_events()
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: otlp_span
                .status
                .as_ref()
                .map(|s| format!("{:?}", s.code()).to_uppercase()),
            status_message: otlp_span.status.as_ref().and_then(|s| {
                if s.message.is_empty() {
                    None
                } else {
                    Some(s.message.clone())
                }
            }),
        };

        // Extract HTTP data from span events and attributes
        span.extract_http_data_from_events();

        Ok(span)
    }

    /// Extract resource attributes from OTLP ResourceSpans
    pub fn extract_resource_attributes(
        resource_spans: &opentelemetry_proto::tonic::trace::v1::ResourceSpans,
    ) -> HashMap<String, String> {
        match &resource_spans.resource {
            Some(resource) => crate::models::key_values_to_map(&resource.attributes),
            None => HashMap::new(),
        }
    }

    /// Extract HTTP data from span events
    /// Looks for `http.request` / `http.response` events and span-attribute fallbacks.
    /// Request/response bodies accept the proxy/SDK keys (`http.request.body`, `http.response.body`)
    /// and the OBI eBPF convention (`http.request.body.content`, `http.response.body.content`);
    /// when both are present, the shorter legacy keys win.
    fn extract_http_data_from_events(&mut self) {
        // Find http.request event
        if let Some(request_event) = self.events.iter().find(|e| e.name == "http.request") {
            self.http_request_headers = request_event
                .attributes
                .get("http.request.headers")
                .cloned();
            self.http_request_body = request_event
                .attributes
                .get("http.request.body")
                .or_else(|| request_event.attributes.get("http.request.body.content"))
                .cloned();
        }

        // Find http.response event
        if let Some(response_event) = self.events.iter().find(|e| e.name == "http.response") {
            self.http_response_headers = response_event
                .attributes
                .get("http.response.headers")
                .cloned();
            self.http_response_body = response_event
                .attributes
                .get("http.response.body")
                .or_else(|| response_event.attributes.get("http.response.body.content"))
                .cloned();
        }

        // Fall back to span attributes when events do not carry HTTP payload fields.
        if self.http_request_headers.is_none() {
            self.http_request_headers = self.attributes.get("http.request.headers").cloned();
        }
        if self.http_request_body.is_none() {
            self.http_request_body = self
                .attributes
                .get("http.request.body")
                .or_else(|| self.attributes.get("http.request.body.content"))
                .cloned();
        }
        if self.http_response_headers.is_none() {
            self.http_response_headers = self.attributes.get("http.response.headers").cloned();
        }
        if self.http_response_body.is_none() {
            self.http_response_body = self
                .attributes
                .get("http.response.body")
                .or_else(|| self.attributes.get("http.response.body.content"))
                .cloned();
        }

        // Extract standard HTTP attributes from span attributes
        self.http_request_method = self.attributes.get("http.request.method").cloned();
        self.http_request_path = self
            .attributes
            .get("http.request.path")
            .or_else(|| self.attributes.get("http.target"))
            .cloned();

        // Extract response status code from span attributes
        if let Some(status_code_str) = self.attributes.get("http.response.status_code") {
            self.http_response_status_code = status_code_str.parse::<i32>().ok();
        } else if let Some(status_code_str) = self.attributes.get("http.status_code") {
            self.http_response_status_code = status_code_str.parse::<i32>().ok();
        }
    }
}

struct CanonicalIngestFields {
    session_id: Option<String>,
    agent_name: Option<String>,
}

fn first_non_empty<'a>(attrs: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| attrs.get(*key).map(|v| v.trim()).filter(|v| !v.is_empty()))
}

fn populate_if_absent(
    attrs: &mut HashMap<String, String>,
    target_key: &str,
    candidate_keys: &[&str],
) {
    if first_non_empty(attrs, &[target_key]).is_some() {
        return;
    }
    if let Some(value) = first_non_empty(attrs, candidate_keys).map(str::to_owned) {
        attrs.insert(target_key.to_string(), value);
    }
}

fn derive_canonical_observation_type(
    span_name: &str,
    attrs: &HashMap<String, String>,
) -> Option<&'static str> {
    use crate::models::attr_keys::{gen_ai, openinference, sp, traceloop, vercel_ai};

    if span_name.trim() == sp::HTTP_BODY_CAPTURE_SPAN_NAME {
        return Some("http_body");
    }

    if let Some(op) = first_non_empty(attrs, &[gen_ai::OPERATION_NAME]) {
        let mapped = match op.to_ascii_lowercase().as_str() {
            "chat" | "generate_content" | "text_completion" => Some("generation"),
            "embeddings" => Some("embedding"),
            "execute_tool" => Some("tool"),
            "invoke_agent" | "create_agent" | "invoke_workflow" => Some("agent"),
            _ => None,
        };
        if mapped.is_some() {
            return mapped;
        }
    }

    if let Some(kind) = first_non_empty(attrs, &[openinference::SPAN_KIND]) {
        let mapped = match kind.to_ascii_uppercase().as_str() {
            "LLM" => Some("generation"),
            "EMBEDDING" => Some("embedding"),
            "TOOL" => Some("tool"),
            "AGENT" => Some("agent"),
            "CHAIN" => Some("chain"),
            "RETRIEVER" | "RERANKER" => Some("retriever"),
            "GUARDRAIL" => Some("guardrail"),
            "EVALUATOR" => Some("evaluator"),
            _ => None,
        };
        if mapped.is_some() {
            return mapped;
        }
    }

    if let Some(kind) = first_non_empty(attrs, &[traceloop::SPAN_KIND]) {
        let mapped = match kind.to_ascii_lowercase().as_str() {
            "agent" => Some("agent"),
            "tool" => Some("tool"),
            "workflow" | "task" => Some("chain"),
            _ => None,
        };
        if mapped.is_some() {
            return mapped;
        }
    }

    if let Some(op_id) = first_non_empty(attrs, &[vercel_ai::OPERATION_ID]) {
        if op_id == "ai.toolCall" {
            return Some("tool");
        }
        if op_id.starts_with("ai.embed") {
            return Some("embedding");
        }
        if op_id.ends_with(".doStream")
            || op_id.ends_with(".doGenerate")
            || matches!(
                op_id,
                "ai.streamText" | "ai.generateText" | "ai.streamObject" | "ai.generateObject"
            )
        {
            return Some("generation");
        }
    }

    None
}

/// Populate promotion source keys only when absent so DuckLake promoted columns and
/// promoted-only `session_summary` compaction read non-`sp.*` spans.
fn canonicalize_multi_standard_llm_attributes(
    span_name: &str,
    attrs: &mut HashMap<String, String>,
) -> CanonicalIngestFields {
    use crate::models::attr_keys::{
        enduser, gen_ai, openinference, otel, sp, traceloop, vercel_ai,
    };

    let resolved_obs_type = match first_non_empty(attrs, &[sp::OBSERVATION_TYPE]) {
        Some(existing) => Some(existing.to_owned()),
        None => derive_canonical_observation_type(span_name, attrs).map(|derived| {
            attrs.insert(sp::OBSERVATION_TYPE.to_string(), derived.to_string());
            derived.to_string()
        }),
    };

    let session_id = first_non_empty(
        attrs,
        &[
            sp::SESSION_ID,
            sp::SESSION_ID_UNDERSCORE,
            otel::SESSION_ID,
            gen_ai::CONVERSATION_ID,
            vercel_ai::SESSION_ID,
            vercel_ai::CONVERSATION_ID,
            traceloop::SESSION_ID,
        ],
    )
    .map(str::to_owned);
    if let Some(ref sid) = session_id {
        if first_non_empty(attrs, &[sp::SESSION_ID]).is_none() {
            attrs.insert(sp::SESSION_ID.to_string(), sid.clone());
        }
    }

    populate_if_absent(
        attrs,
        sp::USER_ID,
        &[
            otel::USER_ID,
            enduser::ID,
            vercel_ai::USER_ID,
            traceloop::USER_ID,
        ],
    );
    populate_if_absent(
        attrs,
        gen_ai::REQUEST_MODEL,
        &[
            gen_ai::RESPONSE_MODEL,
            openinference::LLM_MODEL_NAME,
            vercel_ai::MODEL_ID,
        ],
    );
    populate_if_absent(
        attrs,
        gen_ai::PROVIDER_NAME,
        &[
            gen_ai::SYSTEM,
            openinference::LLM_PROVIDER,
            openinference::LLM_SYSTEM,
            vercel_ai::MODEL_PROVIDER,
        ],
    );
    populate_if_absent(
        attrs,
        gen_ai::USAGE_INPUT_TOKENS,
        &[
            gen_ai::USAGE_PROMPT_TOKENS,
            openinference::TOKEN_COUNT_PROMPT,
            vercel_ai::USAGE_INPUT_TOKENS,
            vercel_ai::USAGE_PROMPT_TOKENS,
        ],
    );
    populate_if_absent(
        attrs,
        gen_ai::USAGE_OUTPUT_TOKENS,
        &[
            gen_ai::USAGE_COMPLETION_TOKENS,
            openinference::TOKEN_COUNT_COMPLETION,
            vercel_ai::USAGE_OUTPUT_TOKENS,
            vercel_ai::USAGE_COMPLETION_TOKENS,
        ],
    );
    populate_if_absent(
        attrs,
        gen_ai::USAGE_TOTAL_TOKENS,
        &[
            openinference::TOKEN_COUNT_TOTAL,
            vercel_ai::USAGE_TOTAL_TOKENS,
        ],
    );

    let agent_name = first_non_empty(attrs, &[gen_ai::AGENT_NAME, sp::AGENT_NAME])
        .map(str::to_owned)
        .or_else(|| {
            if resolved_obs_type.as_deref() == Some("agent") {
                first_non_empty(attrs, &[traceloop::ENTITY_NAME]).map(str::to_owned)
            } else {
                None
            }
        });

    CanonicalIngestFields {
        session_id,
        agent_name,
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_instrumentation_scope, encode_links, Span, SpanEvent};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::span::Link;
    use std::collections::HashMap;

    fn base_span() -> Span {
        Span {
            session_id: "s".to_string(),
            trace_id: "t".to_string(),
            span_id: "sp".to_string(),
            parent_span_id: None,
            app_id: "app".to_string(),
            organization_id: None,
            workspace_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "msg".to_string(),
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
    fn from_otlp_prefers_span_sp_app_id_over_service_name() {
        use opentelemetry_proto::tonic::common::v1::any_value;
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let mut resource = HashMap::new();
        resource.insert("service.name".to_string(), "demo-ota".to_string());

        let otlp = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes: vec![KeyValue {
                key: "sp_app_id".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("travel-ota".into())),
                }),
            }],
            ..Default::default()
        };

        let span = Span::from_otlp(otlp, &resource).expect("from_otlp");
        assert_eq!(span.app_id, "travel-ota");
    }

    #[test]
    fn from_otlp_prefers_resource_sp_app_id_over_service_name() {
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let mut resource = HashMap::new();
        resource.insert("service.name".to_string(), "demo-ota".to_string());
        resource.insert("sp.app.id".to_string(), "travel-ota".to_string());

        let otlp = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            ..Default::default()
        };

        let span = Span::from_otlp(otlp, &resource).expect("from_otlp");
        assert_eq!(span.app_id, "travel-ota");
    }

    #[test]
    fn from_otlp_uses_resource_underscore_sp_app_id() {
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let mut resource = HashMap::new();
        resource.insert("sp_app_id".to_string(), "travel-ota".to_string());
        resource.insert("service.name".to_string(), "demo-ota".to_string());

        let otlp = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            ..Default::default()
        };
        assert_eq!(
            Span::from_otlp(otlp, &resource).expect("from_otlp").app_id,
            "travel-ota"
        );
    }

    #[test]
    fn from_otlp_uses_span_dotted_sp_app_id() {
        use opentelemetry_proto::tonic::common::v1::any_value;
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let mut resource = HashMap::new();
        resource.insert("service.name".to_string(), "demo-ota".to_string());

        let otlp = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes: vec![KeyValue {
                key: "sp.app.id".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("travel-ota".into())),
                }),
            }],
            ..Default::default()
        };
        assert_eq!(
            Span::from_otlp(otlp, &resource).expect("from_otlp").app_id,
            "travel-ota"
        );
    }

    #[test]
    fn from_otlp_uses_gen_ai_conversation_id_as_session() {
        use opentelemetry_proto::tonic::common::v1::any_value;
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let resource = HashMap::new();
        let otlp = OtlpSpan {
            trace_id: vec![9; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes: vec![KeyValue {
                key: "gen_ai.conversation.id".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("conv-only".into())),
                }),
            }],
            ..Default::default()
        };
        assert_eq!(
            Span::from_otlp(otlp, &resource)
                .expect("from_otlp")
                .session_id,
            "conv-only"
        );
    }

    #[test]
    fn from_otlp_normalizes_otel_session_id() {
        use opentelemetry_proto::tonic::common::v1::any_value;
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let resource = HashMap::new();
        let otlp = OtlpSpan {
            trace_id: vec![8; 16],
            span_id: vec![3; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes: vec![KeyValue {
                key: "session.id".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("browser-session".into())),
                }),
            }],
            ..Default::default()
        };

        assert_eq!(
            Span::from_otlp(otlp, &resource)
                .expect("from_otlp")
                .session_id,
            "browser-session"
        );
    }

    #[test]
    fn from_otlp_falls_back_to_service_name() {
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let mut resource = HashMap::new();
        resource.insert("service.name".to_string(), "demo-ota".to_string());

        let otlp = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            ..Default::default()
        };
        assert_eq!(
            Span::from_otlp(otlp, &resource).expect("from_otlp").app_id,
            "demo-ota"
        );
    }

    #[test]
    fn from_otlp_resource_sp_app_id_wins_over_span() {
        use opentelemetry_proto::tonic::common::v1::any_value;
        use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;

        let mut resource = HashMap::new();
        resource.insert("sp.app.id".to_string(), "from-resource".to_string());

        let otlp = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes: vec![KeyValue {
                key: "sp_app_id".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("from-span".into())),
                }),
            }],
            ..Default::default()
        };
        assert_eq!(
            Span::from_otlp(otlp, &resource).expect("from_otlp").app_id,
            "from-resource"
        );
    }

    #[test]
    fn otlp_scope_and_link_carriers_keep_key_value_shape() {
        let scope = InstrumentationScope {
            name: "otel-rust".into(),
            version: "1.0".into(),
            attributes: vec![KeyValue {
                key: "scope.attr".into(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "yes".into(),
                        ),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
        };
        let scope_json: serde_json::Value =
            serde_json::from_str(&encode_instrumentation_scope(&scope)).unwrap();
        assert_eq!(scope_json["attributes"][0]["key"], "scope.attr");

        let links_json: serde_json::Value = serde_json::from_str(&encode_links(&[Link {
            trace_id: vec![1, 2],
            span_id: vec![3, 4],
            trace_state: "vendor=value".into(),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 1,
        }]))
        .unwrap();
        assert_eq!(links_json[0]["traceState"], "vendor=value");
        assert!(links_json[0]["attributes"].is_array());
    }

    #[test]
    fn extract_http_data_falls_back_to_attributes_for_headers_and_bodies() {
        let mut span = base_span();
        span.attributes.insert(
            "http.request.headers".to_string(),
            "{\"x-a\":\"1\"}".to_string(),
        );
        span.attributes
            .insert("http.request.body".to_string(), "{\"in\":true}".to_string());
        span.attributes.insert(
            "http.response.headers".to_string(),
            "{\"content-type\":\"application/json\"}".to_string(),
        );
        span.attributes.insert(
            "http.response.body".to_string(),
            "{\"ok\":true}".to_string(),
        );

        span.extract_http_data_from_events();

        assert_eq!(
            span.http_request_headers.as_deref(),
            Some("{\"x-a\":\"1\"}")
        );
        assert_eq!(span.http_request_body.as_deref(), Some("{\"in\":true}"));
        assert_eq!(
            span.http_response_headers.as_deref(),
            Some("{\"content-type\":\"application/json\"}")
        );
        assert_eq!(span.http_response_body.as_deref(), Some("{\"ok\":true}"));
    }

    #[test]
    fn extract_http_data_prefers_event_values_over_attribute_fallback() {
        let mut span = base_span();
        span.attributes.insert(
            "http.request.body".to_string(),
            "from-attribute".to_string(),
        );
        span.attributes.insert(
            "http.response.body".to_string(),
            "from-attribute".to_string(),
        );

        let mut request_attrs = HashMap::new();
        request_attrs.insert("http.request.body".to_string(), "from-event".to_string());
        let mut response_attrs = HashMap::new();
        response_attrs.insert("http.response.body".to_string(), "from-event".to_string());
        span.events = vec![
            SpanEvent {
                name: "http.request".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: request_attrs,
            },
            SpanEvent {
                name: "http.response".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: response_attrs,
            },
        ];

        span.extract_http_data_from_events();

        assert_eq!(span.http_request_body.as_deref(), Some("from-event"));
        assert_eq!(span.http_response_body.as_deref(), Some("from-event"));
    }

    #[test]
    fn extract_http_data_accepts_obi_body_content_on_span_attributes() {
        let mut span = base_span();
        span.attributes.insert(
            "http.request.body.content".to_string(),
            "{\"obi-req\":true}".to_string(),
        );
        span.attributes.insert(
            "http.response.body.content".to_string(),
            "{\"obi-res\":1}".to_string(),
        );

        span.extract_http_data_from_events();

        assert_eq!(
            span.http_request_body.as_deref(),
            Some("{\"obi-req\":true}")
        );
        assert_eq!(span.http_response_body.as_deref(), Some("{\"obi-res\":1}"));
    }

    #[test]
    fn extract_http_data_accepts_obi_body_content_on_http_events() {
        let mut span = base_span();
        let mut request_attrs = HashMap::new();
        request_attrs.insert(
            "http.request.body.content".to_string(),
            "{\"from-obi-req\":true}".to_string(),
        );
        let mut response_attrs = HashMap::new();
        response_attrs.insert(
            "http.response.body.content".to_string(),
            "{\"from-obi-res\":2}".to_string(),
        );
        span.events = vec![
            SpanEvent {
                name: "http.request".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: request_attrs,
            },
            SpanEvent {
                name: "http.response".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: response_attrs,
            },
        ];

        span.extract_http_data_from_events();

        assert_eq!(
            span.http_request_body.as_deref(),
            Some("{\"from-obi-req\":true}")
        );
        assert_eq!(
            span.http_response_body.as_deref(),
            Some("{\"from-obi-res\":2}")
        );
    }

    #[test]
    fn extract_http_data_prefers_legacy_body_keys_when_both_conventions_present() {
        let mut span = base_span();
        span.attributes
            .insert("http.request.body".to_string(), "legacy-req".to_string());
        span.attributes.insert(
            "http.request.body.content".to_string(),
            "obi-req".to_string(),
        );
        span.attributes
            .insert("http.response.body".to_string(), "legacy-res".to_string());
        span.attributes.insert(
            "http.response.body.content".to_string(),
            "obi-res".to_string(),
        );

        span.extract_http_data_from_events();

        assert_eq!(span.http_request_body.as_deref(), Some("legacy-req"));
        assert_eq!(span.http_response_body.as_deref(), Some("legacy-res"));

        let mut span2 = base_span();
        let mut request_attrs = HashMap::new();
        request_attrs.insert("http.request.body".to_string(), "legacy-req-ev".to_string());
        request_attrs.insert(
            "http.request.body.content".to_string(),
            "obi-req-ev".to_string(),
        );
        let mut response_attrs = HashMap::new();
        response_attrs.insert(
            "http.response.body".to_string(),
            "legacy-res-ev".to_string(),
        );
        response_attrs.insert(
            "http.response.body.content".to_string(),
            "obi-res-ev".to_string(),
        );
        span2.events = vec![
            SpanEvent {
                name: "http.request".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: request_attrs,
            },
            SpanEvent {
                name: "http.response".to_string(),
                timestamp: chrono::Utc::now(),
                attributes: response_attrs,
            },
        ];

        span2.extract_http_data_from_events();

        assert_eq!(span2.http_request_body.as_deref(), Some("legacy-req-ev"));
        assert_eq!(span2.http_response_body.as_deref(), Some("legacy-res-ev"));
    }

    fn str_kv(key: &str, value: &str) -> KeyValue {
        use opentelemetry_proto::tonic::common::v1::any_value;
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn i64_kv(key: &str, value: i64) -> KeyValue {
        use opentelemetry_proto::tonic::common::v1::any_value;
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(value)),
            }),
        }
    }

    fn otlp_span_with_attrs(
        name: &str,
        attributes: Vec<KeyValue>,
    ) -> opentelemetry_proto::tonic::trace::v1::Span {
        opentelemetry_proto::tonic::trace::v1::Span {
            trace_id: vec![0xaa; 16],
            span_id: vec![0xbb; 8],
            name: name.to_string(),
            start_time_unix_nano: 1_721_349_720_000_000_000,
            end_time_unix_nano: 1_721_349_721_000_000_000,
            attributes,
            ..Default::default()
        }
    }

    #[test]
    fn from_otlp_canonicalizes_otel_genai_conventions() {
        let resource = HashMap::new();
        for (op, expected) in [
            ("chat", "generation"),
            ("generate_content", "generation"),
            ("text_completion", "generation"),
            ("embeddings", "embedding"),
            ("execute_tool", "tool"),
            ("invoke_agent", "agent"),
            ("create_agent", "agent"),
            ("invoke_workflow", "agent"),
        ] {
            let span = Span::from_otlp(
                otlp_span_with_attrs(op, vec![str_kv("gen_ai.operation.name", op)]),
                &resource,
            )
            .expect("from_otlp");
            assert_eq!(
                span.attributes
                    .get("sp.observation.type")
                    .map(String::as_str),
                Some(expected),
                "op={op}"
            );
        }

        let span = Span::from_otlp(
            otlp_span_with_attrs(
                "chat gemini-2.5-flash",
                vec![
                    str_kv("gen_ai.operation.name", "chat"),
                    str_kv("gen_ai.conversation.id", "ses_otel_1"),
                    str_kv("user.id", "usr_otel_9"),
                    str_kv("gen_ai.response.model", "gemini-2.5-flash-001"),
                    str_kv("gen_ai.system", "gcp.vertex.agent"),
                    i64_kv("gen_ai.usage.prompt_tokens", 120),
                    i64_kv("gen_ai.usage.completion_tokens", 35),
                ],
            ),
            &resource,
        )
        .expect("from_otlp");

        assert_eq!(span.session_id, "ses_otel_1");
        assert_eq!(
            span.attributes.get("sp.session.id").map(String::as_str),
            Some("ses_otel_1")
        );
        assert_eq!(
            span.attributes.get("sp.user.id").map(String::as_str),
            Some("usr_otel_9")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.request.model")
                .map(String::as_str),
            Some("gemini-2.5-flash-001")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.provider.name")
                .map(String::as_str),
            Some("gcp.vertex.agent")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.usage.input_tokens")
                .map(String::as_str),
            Some("120")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.usage.output_tokens")
                .map(String::as_str),
            Some("35")
        );
    }

    #[test]
    fn from_otlp_canonicalizes_openinference_conventions() {
        let resource = HashMap::new();
        for (kind, expected) in [
            ("LLM", "generation"),
            ("llm", "generation"),
            ("EMBEDDING", "embedding"),
            ("Tool", "tool"),
            ("AGENT", "agent"),
            ("chain", "chain"),
            ("RETRIEVER", "retriever"),
            ("Reranker", "retriever"),
            ("GUARDRAIL", "guardrail"),
            ("evaluator", "evaluator"),
        ] {
            let span = Span::from_otlp(
                otlp_span_with_attrs("oi", vec![str_kv("openinference.span.kind", kind)]),
                &resource,
            )
            .expect("from_otlp");
            assert_eq!(
                span.attributes
                    .get("sp.observation.type")
                    .map(String::as_str),
                Some(expected),
                "kind={kind}"
            );
        }

        let span = Span::from_otlp(
            otlp_span_with_attrs(
                "ChatOpenAI",
                vec![
                    str_kv("openinference.span.kind", "LLM"),
                    str_kv("enduser.id", "oi_user_1"),
                    str_kv("llm.model_name", "claude-sonnet-4"),
                    str_kv("llm.provider", "anthropic"),
                    i64_kv("llm.token_count.prompt", 400),
                    i64_kv("llm.token_count.completion", 80),
                    i64_kv("llm.token_count.total", 510),
                ],
            ),
            &resource,
        )
        .expect("from_otlp");

        assert_eq!(
            span.attributes.get("sp.user.id").map(String::as_str),
            Some("oi_user_1")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.request.model")
                .map(String::as_str),
            Some("claude-sonnet-4")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.provider.name")
                .map(String::as_str),
            Some("anthropic")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.usage.input_tokens")
                .map(String::as_str),
            Some("400")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.usage.output_tokens")
                .map(String::as_str),
            Some("80")
        );
        assert_eq!(
            span.attributes
                .get("gen_ai.usage.total_tokens")
                .map(String::as_str),
            Some("510")
        );
    }

    #[test]
    fn from_otlp_canonicalizes_openllmetry_and_vercel_ai_conventions() {
        let resource = HashMap::new();
        for (kind, expected) in [
            ("agent", "agent"),
            ("TOOL", "tool"),
            ("Workflow", "chain"),
            ("task", "chain"),
        ] {
            let span = Span::from_otlp(
                otlp_span_with_attrs("tl", vec![str_kv("traceloop.span.kind", kind)]),
                &resource,
            )
            .expect("from_otlp");
            assert_eq!(
                span.attributes
                    .get("sp.observation.type")
                    .map(String::as_str),
                Some(expected),
                "traceloop kind={kind}"
            );
        }

        let tl = Span::from_otlp(
            otlp_span_with_attrs(
                "planner.agent",
                vec![
                    str_kv("traceloop.span.kind", "agent"),
                    str_kv("traceloop.association.properties.session_id", "ses_tl_7"),
                    str_kv("traceloop.association.properties.user_id", "usr_tl_7"),
                ],
            ),
            &resource,
        )
        .expect("from_otlp");
        assert_eq!(tl.session_id, "ses_tl_7");
        assert_eq!(
            tl.attributes.get("sp.session.id").map(String::as_str),
            Some("ses_tl_7")
        );
        assert_eq!(
            tl.attributes.get("sp.user.id").map(String::as_str),
            Some("usr_tl_7")
        );

        for (op_id, expected) in [
            ("ai.toolCall", "tool"),
            ("ai.embed", "embedding"),
            ("ai.embedMany.doEmbed", "embedding"),
            ("ai.streamText.doStream", "generation"),
            ("ai.generateText.doGenerate", "generation"),
            ("ai.streamText", "generation"),
            ("ai.generateText", "generation"),
            ("ai.streamObject", "generation"),
            ("ai.generateObject", "generation"),
        ] {
            let span = Span::from_otlp(
                otlp_span_with_attrs(op_id, vec![str_kv("ai.operationId", op_id)]),
                &resource,
            )
            .expect("from_otlp");
            assert_eq!(
                span.attributes
                    .get("sp.observation.type")
                    .map(String::as_str),
                Some(expected),
                "ai.operationId={op_id}"
            );
        }

        let vercel = Span::from_otlp(
            otlp_span_with_attrs(
                "ai.streamText.doStream",
                vec![
                    str_kv("ai.operationId", "ai.streamText.doStream"),
                    str_kv("ai.telemetry.metadata.sessionId", "ses_ai_3"),
                    str_kv("ai.telemetry.metadata.userId", "usr_ai_3"),
                    str_kv("ai.model.id", "gpt-4.1"),
                    str_kv("ai.model.provider", "openai.responses"),
                    i64_kv("ai.usage.inputTokens", 50),
                    i64_kv("ai.usage.outputTokens", 15),
                    i64_kv("ai.usage.totalTokens", 80),
                ],
            ),
            &resource,
        )
        .expect("from_otlp");
        assert_eq!(vercel.session_id, "ses_ai_3");
        assert_eq!(
            vercel
                .attributes
                .get("gen_ai.request.model")
                .map(String::as_str),
            Some("gpt-4.1")
        );
        assert_eq!(
            vercel
                .attributes
                .get("gen_ai.provider.name")
                .map(String::as_str),
            Some("openai.responses")
        );
        assert_eq!(
            vercel
                .attributes
                .get("gen_ai.usage.input_tokens")
                .map(String::as_str),
            Some("50")
        );
        assert_eq!(
            vercel
                .attributes
                .get("gen_ai.usage.output_tokens")
                .map(String::as_str),
            Some("15")
        );
        assert_eq!(
            vercel
                .attributes
                .get("gen_ai.usage.total_tokens")
                .map(String::as_str),
            Some("80")
        );
    }

    #[test]
    fn from_otlp_preserves_explicit_sp_attrs_and_leaves_plain_http_unclassified() {
        let resource = HashMap::new();

        let body_capture = Span::from_otlp(
            otlp_span_with_attrs("softprobe.http.body.capture", vec![]),
            &resource,
        )
        .expect("from_otlp");
        assert_eq!(
            body_capture
                .attributes
                .get("sp.observation.type")
                .map(String::as_str),
            Some("http_body")
        );

        let preserved = Span::from_otlp(
            otlp_span_with_attrs(
                "custom-span",
                vec![
                    str_kv("sp.observation.type", "recording"),
                    str_kv("gen_ai.operation.name", "chat"),
                    str_kv("sp.user.id", "explicit-user"),
                    str_kv("user.id", "other-user"),
                    str_kv("gen_ai.request.model", "explicit-model"),
                    str_kv("llm.model_name", "other-model"),
                    i64_kv("gen_ai.usage.input_tokens", 10),
                    i64_kv("llm.token_count.prompt", 999),
                ],
            ),
            &resource,
        )
        .expect("from_otlp");
        assert_eq!(
            preserved
                .attributes
                .get("sp.observation.type")
                .map(String::as_str),
            Some("recording")
        );
        assert_eq!(
            preserved.attributes.get("sp.user.id").map(String::as_str),
            Some("explicit-user")
        );
        assert_eq!(
            preserved
                .attributes
                .get("gen_ai.request.model")
                .map(String::as_str),
            Some("explicit-model")
        );
        assert_eq!(
            preserved
                .attributes
                .get("gen_ai.usage.input_tokens")
                .map(String::as_str),
            Some("10")
        );

        let plain_http = Span::from_otlp(
            otlp_span_with_attrs(
                "GET /api/flights",
                vec![str_kv("http.request.method", "GET")],
            ),
            &resource,
        )
        .expect("from_otlp");
        assert!(
            !plain_http.attributes.contains_key("sp.observation.type"),
            "ordinary HTTP spans must not be tagged with sp.observation.type at ingest"
        );
        assert!(
            !plain_http.attributes.contains_key("sp.session.id"),
            "trace_id fallback must not synthesize sp.session.id"
        );
    }
}
