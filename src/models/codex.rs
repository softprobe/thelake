use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use chrono::Duration;

use super::{Log, Span};

/// Synthesize canonical Softprobe `Span` records from OpenAI Codex CLI OTLP log events
/// (`codex.user_prompt`, `codex.api_request`, `codex.sse_event`, `codex.tool_result`)
/// so Codex sessions appear in Explorer's Sessions list and Trajectory view.
pub fn synthesize_codex_spans_from_logs(
    logs: &[Log],
    workspace_id: &str,
    agent_id: Option<&str>,
    agent_name: Option<&str>,
) -> Vec<Span> {
    let mut spans = Vec::new();
    for (idx, log) in logs.iter().enumerate() {
        let Some(event_name) = log.attributes.get("event.name").map(|s| s.as_str()) else {
            continue;
        };
        if !event_name.starts_with("codex.") {
            continue;
        }
        let Some(conversation_id) = log
            .attributes
            .get("conversation.id")
            .or(log.session_id.as_ref())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        else {
            continue;
        };

        let trace_id = log
            .trace_id
            .as_deref()
            .filter(|t| t.len() == 32 && t.chars().all(|c| c.is_ascii_hexdigit()))
            .map(|t| t.to_ascii_lowercase())
            .unwrap_or_else(|| deterministic_trace_id(conversation_id));

        let span_id = log
            .span_id
            .as_deref()
            .filter(|s| s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit()))
            .map(|s| s.to_ascii_lowercase())
            .unwrap_or_else(|| {
                deterministic_span_id(
                    conversation_id,
                    event_name,
                    log.timestamp.timestamp_nanos_opt().unwrap_or(0),
                    idx,
                )
            });

        let app_id = log
            .resource_attributes
            .get("service.name")
            .cloned()
            .unwrap_or_else(|| "codex_cli_rs".to_string());

        let mut attrs = HashMap::new();
        attrs.insert("sp.session.id".into(), conversation_id.to_string());
        attrs.insert("gen_ai.conversation.id".into(), conversation_id.to_string());
        attrs.insert("codex.event.name".into(), event_name.to_string());
        if let Some(model) = log.attributes.get("model") {
            attrs.insert("gen_ai.request.model".into(), model.clone());
        }

        let duration_ms = log
            .attributes
            .get("duration_ms")
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|&d| d > 0);
        let end_ts = log.timestamp;
        let start_ts = duration_ms
            .map(|d| end_ts - Duration::milliseconds(d))
            .unwrap_or(end_ts);

        let mut status_code = Some("OK".to_string());
        let mut status_message = None;

        let message_type = match event_name {
            "codex.user_prompt" => {
                attrs.insert("sp.observation.type".into(), "user".into());
                if let Some(prompt) = log
                    .attributes
                    .get("prompt")
                    .or_else(|| (!log.body.is_empty()).then_some(&log.body))
                {
                    if prompt != "[REDACTED]" {
                        attrs.insert("sp.input".into(), prompt.clone());
                    }
                }
                "codex.user".to_string()
            }
            "codex.api_request" => {
                attrs.insert("sp.observation.type".into(), "generation".into());
                attrs.insert("gen_ai.operation.name".into(), "chat".into());
                if let Some(err) = log.attributes.get("error.message").filter(|s| !s.is_empty()) {
                    status_code = Some("ERROR".to_string());
                    status_message = Some(err.clone());
                } else if let Some(code) = log
                    .attributes
                    .get("http.response.status_code")
                    .and_then(|s| s.parse::<u16>().ok())
                {
                    if code >= 400 {
                        status_code = Some("ERROR".to_string());
                        status_message = Some(format!("HTTP {code}"));
                    }
                }
                "codex.generation".to_string()
            }
            "codex.sse_event" => {
                let kind = log
                    .attributes
                    .get("event.kind")
                    .map(|s| s.as_str())
                    .unwrap_or("");
                let has_tokens = log.attributes.contains_key("input_token_count")
                    || log.attributes.contains_key("output_token_count");
                if kind != "response.completed" && !has_tokens {
                    continue;
                }
                attrs.insert("sp.observation.type".into(), "generation".into());
                attrs.insert("gen_ai.operation.name".into(), "chat".into());
                copy_int_attr(&log.attributes, &mut attrs, "input_token_count", "gen_ai.usage.input_tokens");
                copy_int_attr(&log.attributes, &mut attrs, "output_token_count", "gen_ai.usage.output_tokens");
                copy_int_attr(&log.attributes, &mut attrs, "cached_token_count", "gen_ai.usage.cache_read_input_tokens");
                copy_int_attr(&log.attributes, &mut attrs, "reasoning_token_count", "gen_ai.usage.reasoning_tokens");
                if let (Some(inp), Some(out)) = (
                    attrs.get("gen_ai.usage.input_tokens").and_then(|v| v.parse::<i64>().ok()),
                    attrs.get("gen_ai.usage.output_tokens").and_then(|v| v.parse::<i64>().ok()),
                ) {
                    attrs.insert("gen_ai.usage.total_tokens".into(), (inp + out).to_string());
                }
                if log.attributes.get("success").map(|s| s.as_str()) == Some("false") {
                    status_code = Some("ERROR".to_string());
                    status_message = log.attributes.get("error.message").cloned();
                }
                "codex.generation".to_string()
            }
            "codex.tool_result" => {
                let tool_name = log
                    .attributes
                    .get("tool_name")
                    .cloned()
                    .unwrap_or_else(|| "tool".to_string());
                attrs.insert("sp.observation.type".into(), "tool".into());
                attrs.insert("gen_ai.operation.name".into(), "execute_tool".into());
                attrs.insert("gen_ai.tool.name".into(), tool_name.clone());
                if let Some(call_id) = log.attributes.get("call_id") {
                    attrs.insert("gen_ai.tool.call.id".into(), call_id.clone());
                }
                if let Some(args) = log.attributes.get("arguments") {
                    attrs.insert("sp.input".into(), args.clone());
                }
                if let Some(output) = log.attributes.get("output") {
                    attrs.insert("sp.output".into(), output.clone());
                }
                if log.attributes.get("success").map(|s| s.as_str()) == Some("false") {
                    status_code = Some("ERROR".to_string());
                    status_message = log
                        .attributes
                        .get("error.message")
                        .or_else(|| log.attributes.get("output"))
                        .cloned();
                }
                format!("codex.tool.{tool_name}")
            }
            _ => continue,
        };

        spans.push(Span {
            session_id: conversation_id.to_string(),
            trace_id,
            span_id,
            parent_span_id: None,
            app_id,
            organization_id: None,
            workspace_id: Some(workspace_id.to_string()),
            agent_id: agent_id.map(str::to_string),
            agent_name: agent_name.map(str::to_string),
            message_type,
            span_kind: Some("INTERNAL".to_string()),
            timestamp: start_ts,
            end_timestamp: Some(end_ts),
            attributes: attrs,
            resource_attributes: log.resource_attributes.clone(),
            events: Vec::new(),
            status_code,
            status_message,
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
        });
    }
    spans
}

fn copy_int_attr(
    src: &HashMap<String, String>,
    dst: &mut HashMap<String, String>,
    src_key: &str,
    dst_key: &str,
) {
    if let Some(val) = src.get(src_key).filter(|v| !v.is_empty()) {
        dst.insert(dst_key.to_string(), val.clone());
    }
}

fn hash64(seed: u64, value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    value.hash(&mut hasher);
    hasher.finish()
}

fn deterministic_trace_id(conversation_id: &str) -> String {
    let hi = hash64(0x9e37_79b9_7f4a_7c15, conversation_id);
    let lo = hash64(0xc2b2_ae3d_27d4_eb4f, conversation_id);
    format!("{hi:016x}{lo:016x}")
}

fn deterministic_span_id(
    conversation_id: &str,
    event_name: &str,
    ts_nanos: i64,
    idx: usize,
) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    conversation_id.hash(&mut hasher);
    event_name.hash(&mut hasher);
    ts_nanos.hash(&mut hasher);
    idx.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::synthesize_codex_spans_from_logs;
    use crate::models::Log;
    use chrono::Utc;
    use std::collections::HashMap;

    fn codex_log(event_name: &str, extra: &[(&str, &str)]) -> Log {
        let mut attributes: HashMap<String, String> = [
            ("event.name".into(), event_name.into()),
            ("conversation.id".into(), "conv-codex-123".into()),
            ("model".into(), "gpt-5-codex".into()),
        ]
        .into_iter()
        .collect();
        for (k, v) in extra {
            attributes.insert((*k).into(), (*v).into());
        }
        Log {
            session_id: Some("conv-codex-123".into()),
            timestamp: Utc::now(),
            observed_timestamp: None,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: String::new(),
            attributes,
            resource_attributes: [("service.name".into(), "codex_cli_rs".into())]
                .into_iter()
                .collect(),
            trace_id: None,
            span_id: None,
            workspace_id: None,
            agent_id: None,
            agent_name: None,
        }
    }

    #[test]
    fn synthesizes_user_generation_and_tool_spans_from_codex_logs() {
        let logs = vec![
            codex_log("codex.conversation_starts", &[]),
            codex_log("codex.user_prompt", &[("prompt", "Fix failing tests")]),
            codex_log(
                "codex.sse_event",
                &[
                    ("event.kind", "response.completed"),
                    ("input_token_count", "120"),
                    ("output_token_count", "45"),
                ],
            ),
            codex_log(
                "codex.tool_result",
                &[
                    ("tool_name", "shell"),
                    ("arguments", "{\"cmd\":\"npm test\"}"),
                    ("output", "exit 1"),
                    ("success", "false"),
                    ("duration_ms", "250"),
                ],
            ),
        ];

        let spans = synthesize_codex_spans_from_logs(&logs, "ws-1", Some("agent-1"), Some("codex"));
        assert_eq!(spans.len(), 3);

        assert_eq!(spans[0].message_type, "codex.user");
        assert_eq!(spans[0].session_id, "conv-codex-123");
        assert_eq!(spans[0].attributes.get("sp.input").map(String::as_str), Some("Fix failing tests"));

        assert_eq!(spans[1].message_type, "codex.generation");
        assert_eq!(spans[1].attributes.get("sp.observation.type").map(String::as_str), Some("generation"));
        assert_eq!(spans[1].attributes.get("gen_ai.usage.total_tokens").map(String::as_str), Some("165"));

        assert_eq!(spans[2].message_type, "codex.tool.shell");
        assert_eq!(spans[2].attributes.get("gen_ai.tool.name").map(String::as_str), Some("shell"));
        assert_eq!(spans[2].status_code.as_deref(), Some("ERROR"));
        assert_eq!(spans[2].workspace_id.as_deref(), Some("ws-1"));
        assert_eq!(spans[2].agent_id.as_deref(), Some("agent-1"));
    }
}
