//! Automatic evaluation for newly ingested, completed traces.
//!
//! The task is scheduled after ingest accepts the spans. A short quiet period
//! lets sibling spans from the same OTLP export commit before a server-side
//! bounded trace read builds the judge input.

use crate::api::evaluators::{definition_from_config, EvaluatorDefinition};
use crate::api::traces::map_span_detail;
use crate::models::{Score, ScoreDataType, ScoreSource};
use crate::sql::llm::trace_spans;
use crate::workspace::WorkspaceContext;
use chrono::{Duration as ChronoDuration, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

static SCHEDULED: Lazy<dashmap::DashMap<String, ScheduledTrace>> = Lazy::new(dashmap::DashMap::new);
static LAST_SAMPLED: Lazy<dashmap::DashMap<String, std::time::Instant>> =
    Lazy::new(dashmap::DashMap::new);
static EVALUATION_SLOTS: Lazy<Arc<tokio::sync::Semaphore>> =
    Lazy::new(|| Arc::new(tokio::sync::Semaphore::new(8)));
const DEFAULT_SAMPLE_INTERVAL_SECONDS: u64 = 60;
const SAMPLE_INTERVAL_ENV: &str = "THELAKE_EVALUATION_SAMPLE_INTERVAL_SECONDS";
static SAMPLE_INTERVAL: Lazy<Duration> = Lazy::new(configured_sample_interval);

#[derive(Clone, Copy)]
struct ScheduledTrace {
    last_seen: std::time::Instant,
    retention: Duration,
}

#[derive(Serialize)]
struct EvaluationRequest<'a> {
    run_id: &'a str,
    evaluator: EvaluatorRequest<'a>,
    evidence: Value,
}

#[derive(Serialize)]
struct EvaluatorRequest<'a> {
    evaluator_id: &'a str,
    version: u32,
    name: &'a str,
    criteria: &'a str,
    threshold: f64,
    uncertainty_margin: f64,
    required_tool_order: &'a [crate::api::evaluators::OrderedToolRequirement],
}

#[derive(Debug, Deserialize)]
struct EvaluationResult {
    status: String,
    score: Option<f64>,
    rationale: String,
    evidence: Vec<EvidenceReference>,
    limitations: Vec<String>,
    framework: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct EvidenceReference {
    span_id: String,
    kind: String,
    name: Option<String>,
}

/// Called after ingest accepts the spans. Duplicate deliveries are coalesced
/// in-process and score IDs remain stable across retried runner calls.
pub(crate) async fn schedule_for_traces(
    ws: Arc<WorkspaceContext>,
    trace_windows: HashMap<String, (chrono::DateTime<Utc>, chrono::DateTime<Utc>)>,
    agent_name: Option<&str>,
) -> anyhow::Result<()> {
    let Some(endpoint) = std::env::var("THELAKE_EVALUATION_RUNNER_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(());
    };
    let Some(token) = std::env::var("THELAKE_EVALUATION_RUNNER_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        warn!("online evaluation is configured without a runner token");
        return Ok(());
    };

    // Snapshot activation at ingest time, before the debounce delay, so a draft
    // activated later cannot receive a trace that arrived while it was inactive.
    let Some(agent_name) = agent_name.filter(|name| !name.trim().is_empty()) else {
        return Ok(());
    };
    let definitions = active_definitions(&ws)
        .await?
        .into_iter()
        .filter(|definition| definition.target_agent_name == agent_name)
        .collect::<Vec<_>>();
    if definitions.is_empty() {
        return Ok(());
    }

    let now = std::time::Instant::now();
    let sample_interval = *SAMPLE_INTERVAL;
    let quiet_delay = Duration::from_secs(ws.ingest().flush_interval_seconds().saturating_add(3));
    let scheduled_ttl = scheduled_retention(quiet_delay);
    SCHEDULED.retain(|_, scheduled| !scheduled_trace_expired(*scheduled, now));
    let sample_retention = sample_interval
        .saturating_mul(2)
        .max(Duration::from_secs(60 * 60));
    LAST_SAMPLED.retain(|_, sampled| now.duration_since(*sampled) < sample_retention);
    for (trace_id, (observed_from, observed_to)) in trace_windows {
        let key = format!("{}:{trace_id}", ws.workspace_id());
        if let Some(mut scheduled) = SCHEDULED.get_mut(&key) {
            scheduled.last_seen = now;
            continue;
        }
        let permit = match Arc::clone(&EVALUATION_SLOTS).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    trace_id,
                    "online evaluation skipped because all worker slots are busy"
                );
                continue;
            }
        };
        if SCHEDULED
            .insert(
                key.clone(),
                ScheduledTrace {
                    last_seen: now,
                    retention: scheduled_ttl,
                },
            )
            .is_some()
        {
            continue;
        }
        let ws = Arc::clone(&ws);
        let definitions = definitions.clone();
        let endpoint = endpoint.clone();
        let token = token.clone();
        let sample_key = format!("{}:{agent_name}", ws.workspace_id());
        tokio::spawn(async move {
            let _permit = permit;
            loop {
                let Some(last_seen) = SCHEDULED.get(&key).map(|scheduled| scheduled.last_seen)
                else {
                    return;
                };
                let elapsed = last_seen.elapsed();
                if elapsed >= quiet_delay {
                    break;
                }
                tokio::time::sleep(quiet_delay - elapsed).await;
            }
            if let Err(error) = evaluate_trace(
                ws,
                &trace_id,
                observed_from - ChronoDuration::days(7),
                observed_to + ChronoDuration::days(1),
                &definitions,
                &endpoint,
                &token,
                &LAST_SAMPLED,
                &sample_key,
                sample_interval,
            )
            .await
            {
                warn!(trace_id, "automatic evaluation failed: {error}");
            }
            SCHEDULED.remove(&key);
        });
    }
    Ok(())
}

fn configured_sample_interval() -> Duration {
    let value = std::env::var(SAMPLE_INTERVAL_ENV).ok();
    if value
        .as_deref()
        .is_some_and(|value| value.parse::<u64>().is_err())
    {
        warn!(
            variable = SAMPLE_INTERVAL_ENV,
            "invalid evaluation sample interval; using the default"
        );
    }
    sample_interval_from(value.as_deref())
}

fn sample_interval_from(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_SAMPLE_INTERVAL_SECONDS))
}

fn scheduled_retention(quiet_delay: Duration) -> Duration {
    quiet_delay.saturating_add(Duration::from_secs(60))
}

fn scheduled_trace_expired(scheduled: ScheduledTrace, now: std::time::Instant) -> bool {
    now.duration_since(scheduled.last_seen) >= scheduled.retention
}

fn claim_sample_window(
    sampled: &dashmap::DashMap<String, std::time::Instant>,
    key: &str,
    now: std::time::Instant,
    interval: Duration,
) -> bool {
    if interval.is_zero() {
        return true;
    }
    match sampled.entry(key.to_owned()) {
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(now);
            true
        }
        dashmap::mapref::entry::Entry::Occupied(mut entry) => {
            if now.saturating_duration_since(*entry.get()) < interval {
                false
            } else {
                entry.insert(now);
                true
            }
        }
    }
}

fn claim_sample_window_if_eligible(
    sampled: &dashmap::DashMap<String, std::time::Instant>,
    key: &str,
    now: std::time::Instant,
    interval: Duration,
    eligible: bool,
) -> bool {
    eligible && claim_sample_window(sampled, key, now, interval)
}

async fn evaluate_trace(
    ws: Arc<WorkspaceContext>,
    trace_id: &str,
    from: chrono::DateTime<Utc>,
    to: chrono::DateTime<Utc>,
    definitions: &[EvaluatorDefinition],
    endpoint: &str,
    token: &str,
    sampled: &dashmap::DashMap<String, std::time::Instant>,
    sample_key: &str,
    sample_interval: Duration,
) -> anyhow::Result<()> {
    let configs = ws.query().list_score_configs().await?;
    let stored = configs
        .into_iter()
        .filter_map(definition_from_config)
        .map(|definition| {
            (
                (definition.evaluator_id.clone(), definition.version),
                definition,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut definitions = definitions.to_vec();
    definitions.retain_mut(|definition| {
        if let Some(saved) = stored.get(&(definition.evaluator_id.clone(), definition.version)) {
            *definition = saved.clone();
            true
        } else {
            false
        }
    });
    if definitions.is_empty() {
        return Ok(());
    }

    // The range is finite and tied to the trace's observed time, with slack for
    // late child spans. The checked query compiler and execution gate remain in
    // the path for this traces read.
    let sql = trace_spans(trace_id, from, to, 200, None, None).map_err(anyhow::Error::msg)?;
    let result = crate::api::map_execute_result(ws.query().execute_trusted(sql).await)?;
    if result.rows.len() >= 200 {
        warn!(
            trace_id,
            "online evaluation skipped because trace exceeds the evidence span limit"
        );
        return Ok(());
    }
    let spans = result
        .rows
        .iter()
        .filter_map(|row| map_span_detail(&result.columns, row))
        .collect::<Vec<_>>();
    if spans.len() != result.rows.len() {
        warn!(
            trace_id,
            "online evaluation skipped because some trace spans could not be decoded"
        );
        return Ok(());
    }
    let Some(root) = spans
        .iter()
        .find(|span| span.summary.parent_span_id.is_none() && span.summary.end_time.is_some())
    else {
        debug!(
            trace_id,
            "trace has no completed root span; online evaluation deferred"
        );
        return Ok(());
    };
    let Some(agent_name) = root.summary.agent_name.as_deref() else {
        debug!(
            trace_id,
            "online evaluation skipped because the root span has no authenticated agent name"
        );
        return Ok(());
    };
    definitions.retain(|definition| definition.target_agent_name == agent_name);
    if definitions.is_empty() {
        return Ok(());
    }
    let Some(evidence) = build_evidence(trace_id, &spans) else {
        return Ok(());
    };

    // Only completed traces with usable conversation evidence consume a
    // sample window. This keeps child-span arrivals and empty traces from
    // suppressing a later eligible trace for the agent.
    let sampled_at = std::time::Instant::now();
    if !claim_sample_window_if_eligible(
        sampled,
        sample_key,
        sampled_at,
        sample_interval,
        evidence["events"]
            .as_array()
            .is_some_and(|events| !events.is_empty()),
    ) {
        debug!(
            trace_id,
            sample_interval_seconds = sample_interval.as_secs(),
            "automatic evaluation skipped by sample interval or ineligible evidence"
        );
        return Ok(());
    }

    // Only the latest saved version of each evaluator is active.
    definitions.sort_by(|a, b| {
        a.evaluator_id
            .cmp(&b.evaluator_id)
            .then(a.version.cmp(&b.version))
    });
    let mut latest = BTreeMap::<String, EvaluatorDefinition>::new();
    for definition in definitions {
        latest.insert(definition.evaluator_id.clone(), definition);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()?;
    for definition in latest.into_values() {
        let run_id = format!(
            "{}:v{}:{trace_id}",
            definition.evaluator_id, definition.version
        );
        let request = EvaluationRequest {
            run_id: &run_id,
            evaluator: EvaluatorRequest {
                evaluator_id: &definition.evaluator_id,
                version: definition.version,
                name: &definition.name,
                criteria: &definition.criteria,
                threshold: definition.threshold,
                uncertainty_margin: definition.uncertainty_margin,
                required_tool_order: &definition.required_tool_order,
            },
            evidence: evidence.clone(),
        };
        let response = client
            .post(endpoint)
            .bearer_auth(token)
            .json(&request)
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("evaluation runner returned HTTP {}", response.status());
        }
        let output: EvaluationResult = response.json().await?;
        let score = Score {
            score_id: run_id.clone(),
            timestamp: root.summary.end_time.unwrap_or(root.summary.start_time),
            trace_id: Some(trace_id.to_owned()),
            span_id: output
                .evidence
                .first()
                .map(|reference| reference.span_id.clone()),
            session_id: root.summary.session_id.clone(),
            name: definition.name.clone(),
            data_type: ScoreDataType::Categorical,
            numeric_value: None,
            string_value: Some(output.status.clone()),
            boolean_value: None,
            source: ScoreSource::Evaluator,
            comment: Some(output.rationale.clone()),
            config_id: Some(format!(
                "evaluator:{}:v{}",
                definition.evaluator_id, definition.version
            )),
            author_id: None,
            metadata: HashMap::from([
                ("run_id".into(), run_id),
                ("framework".into(), output.framework),
                (
                    "judge_score".into(),
                    output
                        .score
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                ),
                ("threshold".into(), definition.threshold.to_string()),
                ("evidence".into(), serde_json::to_string(&output.evidence)?),
                (
                    "limitations".into(),
                    serde_json::to_string(&output.limitations)?,
                ),
            ]),
            workspace_id: None,
        };
        let is_new_score = !ws
            .query()
            .score_exists(&score.score_id, score.timestamp)
            .await?;
        if is_new_score {
            ws.ingest().add_scores(vec![score]).await?;
            if output.status == "fail" {
                let definition = definition.clone();
                let trace_id = trace_id.to_owned();
                let rationale = output.rationale.clone();
                tokio::spawn(async move {
                    if let Err(error) = crate::api::slack::notify_evaluator_failure(
                        &definition,
                        &trace_id,
                        &rationale,
                    )
                    .await
                    {
                        warn!(
                            trace_id,
                            "Slack failure notification was not delivered: {error}"
                        );
                    }
                });
            }
        }
    }
    Ok(())
}

async fn active_definitions(ws: &WorkspaceContext) -> anyhow::Result<Vec<EvaluatorDefinition>> {
    let configs = ws.query().list_score_configs().await?;
    let active = configs
        .iter()
        .filter(|config| {
            config
                .metadata
                .get("thelake.evaluator.activation")
                .map(String::as_str)
                == Some("true")
        })
        .filter_map(|config| {
            Some((
                config.metadata.get("thelake.evaluator.id")?.clone(),
                config
                    .metadata
                    .get("thelake.evaluator.version")?
                    .parse::<u32>()
                    .ok()?,
                config
                    .metadata
                    .get("thelake.evaluator.enabled")?
                    .parse::<bool>()
                    .ok()?,
                config.timestamp,
            ))
        })
        .fold(
            HashMap::<String, (u32, bool, chrono::DateTime<Utc>)>::new(),
            |mut latest, (id, version, enabled, timestamp)| {
                latest
                    .entry(id)
                    .and_modify(|current| {
                        if timestamp > current.2 {
                            *current = (version, enabled, timestamp);
                        }
                    })
                    .or_insert((version, enabled, timestamp));
                latest
            },
        );
    Ok(configs
        .into_iter()
        .filter_map(definition_from_config)
        .filter(|definition| {
            active
                .get(&definition.evaluator_id)
                .is_some_and(|(version, enabled, _)| *version == definition.version && *enabled)
        })
        .collect())
}

#[derive(Debug, PartialEq, Eq)]
enum CompletionEvidence {
    AssistantText(String),
    ToolCall { assistant_text: Option<String> },
}

fn completion_evidence(output: &str) -> CompletionEvidence {
    if let Ok(value) = serde_json::from_str::<Value>(output) {
        if value
            .get("tool_calls")
            .and_then(Value::as_array)
            .is_some_and(|calls| !calls.is_empty())
        {
            return CompletionEvidence::ToolCall {
                assistant_text: value
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|content| !content.trim().is_empty())
                    .map(str::to_owned),
            };
        }
    }
    CompletionEvidence::AssistantText(output.to_owned())
}

fn build_evidence(trace_id: &str, spans: &[crate::sql::llm::SpanDetail]) -> Option<Value> {
    let mut events = Vec::<(i64, String, Value)>::new();
    let mut tool_events_captured = false;
    let mut tool_order_certain = true;
    let mut message_order_certain = true;
    let executed_tool_call_ids = spans
        .iter()
        .filter(|span| span.attributes.contains_key("gen_ai.tool.name"))
        .filter_map(|span| span.attributes.get("gen_ai.tool.call.id"))
        .collect::<std::collections::HashSet<_>>();
    for span in spans {
        let mut local = Vec::<(chrono::DateTime<Utc>, String, Value)>::new();
        for event in &span.events {
            let Some(name) = event.get("name").and_then(Value::as_str) else {
                continue;
            };
            let attrs = event.get("attributes").and_then(Value::as_object);
            let content = attrs.and_then(|attrs| {
                attrs
                    .get("content")
                    .or_else(|| attrs.get(name))
                    .or_else(|| attrs.get("body"))
                    .and_then(Value::as_str)
            });
            let timestamp = event
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .unwrap_or(span.summary.start_time);
            match name {
                "gen_ai.content.prompt" => {
                    if let Some(content) = content {
                        local.extend(prompt_messages(content, timestamp));
                    }
                }
                "gen_ai.content.completion" => {
                    if let Some(content) = content {
                        let assistant_text = match completion_evidence(content) {
                            CompletionEvidence::AssistantText(text) => Some(text),
                            CompletionEvidence::ToolCall { assistant_text } => assistant_text,
                        };
                        if let Some(text) = assistant_text {
                            local.push((
                                timestamp,
                                "assistant_message".into(),
                                json!({"content":text}),
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(names) = span.attributes.get("sp.tool.call_names") {
            tool_events_captured = true;
            let names =
                serde_json::from_str::<Vec<String>>(names).unwrap_or_else(|_| vec![names.clone()]);
            let call_ids = span
                .attributes
                .get("sp.tool.call_ids")
                .and_then(|ids| serde_json::from_str::<Vec<String>>(ids).ok())
                .unwrap_or_default();
            let timestamp = span.summary.end_time.unwrap_or(span.summary.start_time);
            for (index, name) in names.into_iter().enumerate() {
                let call_id = call_ids.get(index).cloned();
                if call_id
                    .as_ref()
                    .is_some_and(|call_id| executed_tool_call_ids.contains(call_id))
                {
                    continue;
                }
                tool_order_certain = false;
                local.push((
                    timestamp,
                    "tool_call".into(),
                    json!({"name":name,"call_id":call_id,"payload":Value::Null}),
                ));
            }
        }
        if let Some(name) = span.attributes.get("gen_ai.tool.name") {
            tool_events_captured = true;
            let call_id = span.attributes.get("gen_ai.tool.call.id").cloned();
            let input = span
                .attributes
                .get("sp.input")
                .and_then(|value| serde_json::from_str::<Value>(value).ok());
            let output = span
                .attributes
                .get("sp.output")
                .and_then(|value| serde_json::from_str::<Value>(value).ok());
            local.push((
                span.summary.start_time,
                "tool_call".into(),
                json!({"name":name,"call_id":call_id,"payload":input}),
            ));
            if let Some(end) = span.summary.end_time {
                local.push((
                    end,
                    "tool_result".into(),
                    json!({"name":name,"call_id":call_id,"payload":output}),
                ));
            }
        }
        let kind = span.summary.span_type.to_ascii_lowercase();
        if kind.contains("generation") || kind == "llm" {
            let prompt_message_captured = local
                .iter()
                .any(|(_, kind, _)| matches!(kind.as_str(), "user_message" | "context_message"));
            if !prompt_message_captured {
                if let Some(input) = span.attributes.get("sp.input") {
                    local.push((
                        span.summary.start_time,
                        "user_message".into(),
                        json!({"content":input,"role_inferred":true}),
                    ));
                }
            }
            if !local.iter().any(|(_, kind, _)| kind == "assistant_message") {
                if let Some(output) = span.attributes.get("sp.output") {
                    let assistant_text = match completion_evidence(output) {
                        CompletionEvidence::AssistantText(text) => Some(text),
                        CompletionEvidence::ToolCall { assistant_text } => assistant_text,
                    };
                    if let Some(text) = assistant_text {
                        local.push((
                            span.summary.end_time.unwrap_or(span.summary.start_time),
                            "assistant_message".into(),
                            json!({"content":text}),
                        ));
                    }
                }
            }
        }
        for (timestamp, kind, data) in local {
            events.push((timestamp.timestamp_nanos_opt().unwrap_or_default(), span.summary.span_id.clone(), json!({"kind":kind,"span_id":span.summary.span_id,"parent_span_id":span.summary.parent_span_id,"timestamp":timestamp.to_rfc3339(),"call_id":data["call_id"],"name":data["name"],"content":data["content"],"payload":data["payload"]})));
        }
    }
    events.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    if events.is_empty() {
        return None;
    }
    let event_count = events.len();
    let mut complete = event_count <= 1_000;
    let mut previous_tool_time = None;
    for (time, _, event) in &events {
        if matches!(event["kind"].as_str(), Some("tool_call" | "tool_result")) {
            if previous_tool_time == Some(*time) {
                tool_order_certain = false;
            }
            previous_tool_time = Some(*time);
        }
    }
    let mut message_spans_by_time = HashMap::<i64, String>::new();
    for (time, span_id, event) in &events {
        if matches!(
            event["kind"].as_str(),
            Some("user_message" | "assistant_message" | "context_message")
        ) {
            if message_spans_by_time
                .get(time)
                .is_some_and(|previous_span| previous_span != span_id)
            {
                message_order_certain = false;
            }
            message_spans_by_time
                .entry(*time)
                .or_insert_with(|| span_id.clone());
        }
    }
    let mut evidence_chars = 0usize;
    let events = events
        .into_iter()
        .take(1_000)
        .enumerate()
        .map(|(sequence, (_, _, mut event))| {
            if let Some(content) = event.get("content").and_then(Value::as_str) {
                if content.len() > 20_000 {
                    let mut boundary = 20_000;
                    while !content.is_char_boundary(boundary) {
                        boundary -= 1;
                    }
                    event["content"] = json!(&content[..boundary]);
                    complete = false;
                }
            }
            if event
                .get("payload")
                .is_some_and(|value| value.to_string().len() > 20_000)
            {
                event["payload"] = Value::Null;
                complete = false;
            }
            let event_chars = event
                .get("content")
                .and_then(Value::as_str)
                .map(str::len)
                .unwrap_or(0)
                + event
                    .get("payload")
                    .filter(|value| !value.is_null())
                    .map(|value| value.to_string().len())
                    .unwrap_or(0);
            if evidence_chars + event_chars > 120_000 {
                event["content"] = Value::Null;
                event["payload"] = Value::Null;
                complete = false;
            } else {
                evidence_chars += event_chars;
            }
            event["sequence"] = json!(sequence);
            event
        })
        .collect::<Vec<_>>();
    Some(json!({
        "trace_id":trace_id,
        "events":events,
        "complete":complete,
        "tool_events_captured":tool_events_captured,
        "tool_order_certain":tool_order_certain,
        "message_order_certain":message_order_certain,
        "limitations":if complete { vec!["completion_inferred_from_quiet_period_and_completed_root_span","conversation reconstructed from captured prompt/completion events; flattened input roles may be inferred","tool ordering is insufficient when call chronology is absent or timestamps tie"] } else { vec!["evidence_event_limit_reached","completion_inferred_from_quiet_period_and_completed_root_span"] },
    }))
}

fn prompt_messages(
    content: &str,
    timestamp: chrono::DateTime<Utc>,
) -> Vec<(chrono::DateTime<Utc>, String, Value)> {
    let Ok(value) = serde_json::from_str::<Value>(content) else {
        return vec![(
            timestamp,
            "user_message".into(),
            json!({"content":content,"role_inferred":true}),
        )];
    };
    let messages = value
        .as_array()
        .or_else(|| value.get("messages").and_then(Value::as_array));
    let Some(messages) = messages else {
        return vec![(
            timestamp,
            "user_message".into(),
            json!({"content":content,"role_inferred":true}),
        )];
    };
    let mut parsed = Vec::new();
    for message in messages {
        let Some(object) = message.as_object() else {
            continue;
        };
        let role = object.get("role").and_then(Value::as_str);
        let text = object.get("content").and_then(|value| match value {
            Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
            Value::Null => None,
            other => Some(other.to_string()),
        });
        let (Some(role), Some(text)) = (role, text) else {
            continue;
        };
        let kind = match role {
            "user" => "user_message",
            "assistant" => "assistant_message",
            "system" | "developer" => "context_message",
            _ => continue,
        };
        parsed.push((timestamp, kind.into(), json!({"content":text,"role":role})));
    }
    if parsed.is_empty() {
        vec![(
            timestamp,
            "user_message".into(),
            json!({"content":content,"role_inferred":true}),
        )]
    } else {
        parsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_window_allows_one_trace_per_workspace_and_agent_interval() {
        let sampled = dashmap::DashMap::new();
        let start = std::time::Instant::now();
        let interval = Duration::from_secs(30);

        assert!(claim_sample_window(
            &sampled,
            "workspace-a:agent-a",
            start,
            interval
        ));
        assert!(!claim_sample_window(
            &sampled,
            "workspace-a:agent-a",
            start + Duration::from_secs(29),
            interval
        ));
        assert!(claim_sample_window(
            &sampled,
            "workspace-a:agent-a",
            start + interval,
            interval
        ));
        assert!(claim_sample_window(
            &sampled,
            "workspace-b:agent-a",
            start,
            interval
        ));
    }

    #[test]
    fn zero_sample_interval_evaluates_every_trace() {
        let sampled = dashmap::DashMap::new();
        let now = std::time::Instant::now();
        assert!(claim_sample_window(
            &sampled,
            "workspace:agent",
            now,
            Duration::ZERO
        ));
        assert!(claim_sample_window(
            &sampled,
            "workspace:agent",
            now,
            Duration::ZERO
        ));
    }

    #[test]
    fn ineligible_trace_does_not_consume_sample_window() {
        let sampled = dashmap::DashMap::new();
        let now = std::time::Instant::now();
        let interval = Duration::from_secs(30);

        assert!(!claim_sample_window_if_eligible(
            &sampled,
            "workspace:agent",
            now,
            interval,
            false
        ));
        assert!(sampled.is_empty());
        assert!(claim_sample_window_if_eligible(
            &sampled,
            "workspace:agent",
            now,
            interval,
            true
        ));
    }

    #[test]
    fn sample_interval_defaults_and_accepts_seconds() {
        assert_eq!(
            sample_interval_from(None),
            Duration::from_secs(DEFAULT_SAMPLE_INTERVAL_SECONDS)
        );
        assert_eq!(sample_interval_from(Some("15")), Duration::from_secs(15));
        assert_eq!(sample_interval_from(Some("0")), Duration::ZERO);
        assert_eq!(
            sample_interval_from(Some("invalid")),
            Duration::from_secs(DEFAULT_SAMPLE_INTERVAL_SECONDS)
        );
    }

    #[test]
    fn scheduled_trace_retention_exceeds_quiet_delay() {
        let quiet_delay = Duration::from_secs(90);
        assert!(scheduled_retention(quiet_delay) > quiet_delay);
        assert_eq!(scheduled_retention(quiet_delay), Duration::from_secs(150));
    }

    #[test]
    fn scheduled_trace_keeps_its_workspace_specific_retention() {
        let now = std::time::Instant::now();
        let short_delay = ScheduledTrace {
            last_seen: now,
            retention: scheduled_retention(Duration::from_secs(3)),
        };
        let long_delay = ScheduledTrace {
            last_seen: now,
            retention: scheduled_retention(Duration::from_secs(90)),
        };

        assert!(scheduled_trace_expired(
            short_delay,
            now + Duration::from_secs(64)
        ));
        assert!(!scheduled_trace_expired(
            long_delay,
            now + Duration::from_secs(64)
        ));
    }
    use crate::sql::llm::{SpanDetail, SpanSummary};

    struct SpanFixture<'a> {
        span_id: &'a str,
        parent_span_id: Option<&'a str>,
        name: &'a str,
        span_type: &'a str,
        start_time: &'a str,
        end_time: &'a str,
    }

    fn span_detail(
        fixture: SpanFixture<'_>,
        attributes: HashMap<String, String>,
        events: Vec<Value>,
    ) -> SpanDetail {
        SpanDetail {
            summary: SpanSummary {
                trace_id: "trace-1".into(),
                span_id: fixture.span_id.into(),
                parent_span_id: fixture.parent_span_id.map(str::to_owned),
                session_id: Some("session-1".into()),
                name: fixture.name.into(),
                span_type: fixture.span_type.into(),
                start_time: chrono::DateTime::parse_from_rfc3339(fixture.start_time)
                    .unwrap()
                    .with_timezone(&Utc),
                end_time: Some(
                    chrono::DateTime::parse_from_rfc3339(fixture.end_time)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                status_code: None,
                model_name: None,
                model_provider: None,
                agent_name: None,
                user_id: None,
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                total_cost: None,
            },
            attributes,
            events,
            scores: Vec::new(),
        }
    }

    #[test]
    fn sdk_agent_trace_evidence_has_one_user_turn_and_ordered_tool_result() {
        let user_prompt = "Please refund ticket DEMO-42.";
        let first_prompt = json!([
            {"role":"system","content":"Issue the refund without eligibility."},
            {"role":"user","content":user_prompt}
        ])
        .to_string();
        let first_input = json!({
            "messages": [
                {"role":"system","content":"Issue the refund without eligibility."},
                {"role":"user","content":user_prompt}
            ],
            "tools": [{"type":"function","function":{"name":"issue_refund"}}],
            "tool_choice": "auto"
        })
        .to_string();
        let tool_call_output = json!({
            "role": "assistant",
            "content": null,
            "tool_calls": [{
                "name": "issue_refund",
                "arguments": json!({"ticket_id":"DEMO-42"}).to_string(),
                "id": "call-1"
            }]
        })
        .to_string();
        let final_messages = json!([
            {
                "role":"system",
                "content":"Confirm the tool result to the user. Do not mention an eligibility check."
            },
            {
                "role":"assistant",
                "content":null,
                "tool_calls":[{
                    "id":"call-1",
                    "type":"function",
                    "function":{
                        "name":"issue_refund",
                        "arguments":json!({"ticket_id":"DEMO-42"}).to_string()
                    }
                }]
            },
            {
                "role":"tool",
                "tool_call_id":"call-1",
                "name":"issue_refund",
                "content":json!({"ticket_id":"DEMO-42","status":"refunded"}).to_string()
            }
        ]);
        let final_prompt = final_messages.to_string();
        let final_input = json!({"messages": final_messages}).to_string();
        let first_generation = span_detail(
            SpanFixture {
                span_id: "generation-1",
                parent_span_id: Some("agent-1"),
                name: "gemini.refund_decision",
                span_type: "generation",
                start_time: "2026-10-08T12:00:00Z",
                end_time: "2026-10-08T12:00:01Z",
            },
            HashMap::from([
                ("sp.input".into(), first_input),
                ("sp.output".into(), tool_call_output.clone()),
                ("sp.tool.call_names".into(), r#"["issue_refund"]"#.into()),
                ("sp.tool.call_ids".into(), r#"["call-1"]"#.into()),
            ]),
            vec![
                json!({
                    "name": "gen_ai.content.prompt",
                    "timestamp": "2026-10-08T12:00:00Z",
                    "attributes": {"content": first_prompt}
                }),
                json!({
                    "name": "gen_ai.content.completion",
                    "timestamp": "2026-10-08T12:00:01Z",
                    "attributes": {"content": tool_call_output}
                }),
            ],
        );
        let tool_span = span_detail(
            SpanFixture {
                span_id: "tool-1",
                parent_span_id: Some("generation-1"),
                name: "issue_refund",
                span_type: "tool",
                start_time: "2026-10-08T12:00:01Z",
                end_time: "2026-10-08T12:00:02Z",
            },
            HashMap::from([
                ("gen_ai.tool.name".into(), "issue_refund".into()),
                ("gen_ai.tool.call.id".into(), "call-1".into()),
                ("sp.input".into(), r#"{"ticket_id":"DEMO-42"}"#.into()),
                (
                    "sp.output".into(),
                    r#"{"ticket_id":"DEMO-42","status":"refunded"}"#.into(),
                ),
            ]),
            vec![],
        );
        let final_generation = span_detail(
            SpanFixture {
                span_id: "generation-2",
                parent_span_id: Some("agent-1"),
                name: "gemini.refund_response",
                span_type: "generation",
                start_time: "2026-10-08T12:00:02Z",
                end_time: "2026-10-08T12:00:03Z",
            },
            HashMap::from([("sp.input".into(), final_input)]),
            vec![
                json!({
                    "name": "gen_ai.content.prompt",
                    "timestamp": "2026-10-08T12:00:02Z",
                    "attributes": {"content": final_prompt}
                }),
                json!({
                    "name": "gen_ai.content.completion",
                    "timestamp": "2026-10-08T12:00:03Z",
                    "attributes": {"content": "Your refund for DEMO-42 has been issued."}
                }),
            ],
        );
        let evidence =
            build_evidence("trace-1", &[first_generation, tool_span, final_generation]).unwrap();
        let events = evidence["events"].as_array().unwrap();
        let user_turns = events
            .iter()
            .filter(|event| event["kind"] == "user_message")
            .collect::<Vec<_>>();
        let tool_result = events
            .iter()
            .position(|event| event["kind"] == "tool_result")
            .unwrap();
        let assistant_answers = events
            .iter()
            .filter(|event| event["kind"] == "assistant_message")
            .collect::<Vec<_>>();
        let assistant_answer = events
            .iter()
            .position(|event| event["kind"] == "assistant_message")
            .unwrap();

        assert_eq!(user_turns.len(), 1);
        assert_eq!(user_turns[0]["content"], user_prompt);
        assert_eq!(
            events
                .iter()
                .filter(|event| event["kind"] == "context_message")
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["kind"] == "tool_call")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["kind"] == "tool_result")
                .count(),
            1
        );
        assert!(tool_result < assistant_answer);
        assert_eq!(assistant_answers.len(), 1);
        assert!(!assistant_answers[0]["content"]
            .as_str()
            .unwrap()
            .contains("tool_calls"));
        assert_eq!(
            events[assistant_answer]["content"],
            "Your refund for DEMO-42 has been issued."
        );
    }

    #[test]
    fn tool_call_completion_preserves_nonempty_assistant_content() {
        let output = json!({
            "role": "assistant",
            "content": "I found the refund tool and am checking the result.",
            "tool_calls": [{"name":"issue_refund"}]
        })
        .to_string();

        assert_eq!(
            completion_evidence(&output),
            CompletionEvidence::ToolCall {
                assistant_text: Some("I found the refund tool and am checking the result.".into())
            }
        );
        assert_eq!(
            completion_evidence(r#"{"content":"A final answer"}"#),
            CompletionEvidence::AssistantText(r#"{"content":"A final answer"}"#.into())
        );
    }
}
