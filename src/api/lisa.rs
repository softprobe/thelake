use crate::api::error::{bad_request, storage_error, ApiError};
use crate::api::evaluators::{self, CreateEvaluatorRequest, EvaluatorDefinition};
use crate::api::scores::CreateScoreRequest;
use crate::api::sessions::{SessionOrderBy, SessionSearchRequest, SortDirection};
use crate::api::AppState;
use crate::authn::WorkspaceAuth;
use axum::extract::{Extension, State};
use axum::{
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;

const RECENT_SESSION_LIMIT: usize = 25;
const SESSION_EVIDENCE_LIMIT: usize = 1;
const SESSION_SPAN_LIMIT: usize = 20;
const SESSION_EVALUATION_SPAN_LIMIT: usize = 500;
type HmacSha256 = Hmac<Sha256>;

async fn recent_sessions(state: &AppState, auth: &WorkspaceAuth) -> Result<Value, ApiError> {
    let workspace = state
        .workspace_for_auth(auth)
        .await
        .map_err(storage_error)?;
    let to = Utc::now();
    let request = SessionSearchRequest {
        from: to - Duration::days(7),
        to,
        has_errors: None,
        user_id: None,
        model_name: None,
        agent_name: None,
        roots_only: true,
        order_by: SessionOrderBy::StartTime,
        order: SortDirection::Desc,
        limit: Some(RECENT_SESSION_LIMIT),
        cursor: None,
    };
    let response = workspace
        .search_session_summary(&request, RECENT_SESSION_LIMIT)
        .await
        .map_err(|error| match error {
            crate::session_summary::SessionSummaryListError::BadRequest(message) => {
                bad_request(message)
            }
            crate::session_summary::SessionSummaryListError::Storage(error) => storage_error(error),
        })?;
    serde_json::to_value(json!({ "sessions": response.items, "session_evidence": [] }))
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "could not prepare Lisa's session context" })),
            )
        })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSessionSearchRequest {
    #[serde(default)]
    query: String,
    #[serde(default)]
    errors_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSessionEvidenceRequest {
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpEvaluatorDraftRequest {
    evaluator: CreateEvaluatorRequest,
    #[serde(default)]
    slack_channel_id: Option<String>,
    #[serde(default)]
    slack_thread_ts: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpEvaluatorActivationRequest {
    evaluator_id: String,
    version: u32,
    name: String,
    target_agent_name: String,
}

fn filter_session_summaries(
    sessions: Vec<Value>,
    query: &str,
    errors_only: bool,
) -> Vec<Value> {
    let stop_words = [
        "find", "show", "me", "the", "latest", "recent", "sessions", "session", "with",
        "for", "agent",
    ];
    let terms = query
        .to_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|term| term.len() > 2 && !stop_words.contains(term))
        .map(str::to_string)
        .collect::<Vec<_>>();
    sessions
        .into_iter()
        .filter(|session| {
            let is_error = session
                .get("error_count")
                .and_then(Value::as_i64)
                .unwrap_or_default()
                > 0;
            let serialized = session.to_string().to_lowercase();
            (!errors_only || is_error) && terms.iter().all(|term| serialized.contains(term))
        })
        .collect()
}

fn require_lisa_service(headers: &HeaderMap) -> Result<WorkspaceAuth, ApiError> {
    let configured = std::env::var("LISA_THELAKE_TOKEN").unwrap_or_default();
    let supplied = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("");
    let workspace_id = headers
        .get("X-TheLake-Workspace-ID")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let signature = headers
        .get("X-TheLake-Workspace-Signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let authorized = verify_service_token(&configured, supplied)
        && verify_workspace_binding(&configured, workspace_id, signature);
    if !authorized {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Lisa service authentication required"})),
        ));
    }
    Ok(WorkspaceAuth {
        workspace_id: workspace_id.to_owned(),
        bucket_name: String::new(),
        dataset_id: String::new(),
        agent_id: None,
        agent_name: None,
    })
}

fn verify_workspace_binding(token: &str, workspace_id: &str, signature: &str) -> bool {
    let Ok(workspace_id) = uuid::Uuid::parse_str(workspace_id) else {
        return false;
    };
    let Ok(signature) = hex::decode(signature) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(token.as_bytes()) else {
        return false;
    };
    mac.update(b"thelake-lisa-workspace-v1:");
    mac.update(workspace_id.to_string().as_bytes());
    mac.verify_slice(&signature).is_ok()
}

fn verify_service_token(configured: &str, supplied: &str) -> bool {
    if configured.is_empty() || supplied.is_empty() {
        return false;
    }
    let Ok(mut expected_mac) = HmacSha256::new_from_slice(configured.as_bytes()) else {
        return false;
    };
    expected_mac.update(configured.as_bytes());
    let expected = expected_mac.finalize().into_bytes();
    let Ok(mut actual) = HmacSha256::new_from_slice(configured.as_bytes()) else {
        return false;
    };
    actual.update(supplied.as_bytes());
    actual.verify_slice(&expected).is_ok()
}

pub async fn mcp_search_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<McpSessionSearchRequest>,
) -> Result<Json<Value>, ApiError> {
    let auth = require_lisa_service(&headers)?;
    let mut context = recent_sessions(&state, &auth).await?;
    let sessions = context
        .get("sessions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let filtered = filter_session_summaries(sessions, &request.query, request.errors_only);
    let mut evidence = Vec::new();
    for summary in filtered.iter().take(SESSION_EVIDENCE_LIMIT) {
        let Some(session_id) = summary.get("session_id").and_then(Value::as_str) else {
            continue;
        };
        if let Ok(detail) = crate::api::sessions::session_detail_for_workspace(
            &state,
            Some(&auth),
            session_id,
        )
        .await
        {
            evidence.push(compact_session_evidence(detail));
        }
    }
    context["sessions"] = Value::Array(filtered);
    context["session_evidence"] = Value::Array(evidence);
    Ok(Json(context))
}

pub async fn mcp_session_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<McpSessionEvidenceRequest>,
) -> Result<Json<Value>, ApiError> {
    let auth = require_lisa_service(&headers)?;
    let detail = crate::api::sessions::session_detail_for_workspace(
        &state,
        Some(&auth),
        &request.session_id,
    )
    .await?;
    Ok(Json(session_evidence(
        detail,
        SESSION_EVALUATION_SPAN_LIMIT,
    )))
}

pub async fn mcp_active_evaluators(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let auth = require_lisa_service(&headers)?;
    let Json(definitions) =
        evaluators::list_evaluators(State(state), Some(Extension(auth))).await?;
    let items = definitions
        .into_iter()
        .filter(|definition| definition.active)
        .map(|definition: EvaluatorDefinition| {
            let mut value = serde_json::to_value(&definition).unwrap_or(Value::Null);
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "slack_channel_id".into(),
                    serde_json::to_value(definition.slack_channel_id).unwrap_or(Value::Null),
                );
                object.insert(
                    "slack_thread_ts".into(),
                    serde_json::to_value(definition.slack_thread_ts).unwrap_or(Value::Null),
                );
            }
            value
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({ "items": items })))
}

pub async fn mcp_create_score(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateScoreRequest>,
) -> Result<(StatusCode, Json<crate::models::Score>), ApiError> {
    let auth = require_lisa_service(&headers)?;
    crate::api::scores::create_score(State(state), Some(Extension(auth)), Json(request)).await
}

pub async fn mcp_create_evaluator_draft(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<McpEvaluatorDraftRequest>,
) -> Result<(StatusCode, Json<evaluators::EvaluatorDefinition>), ApiError> {
    let auth = require_lisa_service(&headers)?;
    let workspace = state
        .workspace_for_auth(&auth)
        .await
        .map_err(storage_error)?;
    let mut evaluator = request.evaluator;
    evaluator.slack_channel_id = request.slack_channel_id;
    evaluator.slack_thread_ts = request.slack_thread_ts;
    if evaluator.version == 0 {
        let existing = workspace
            .query()
            .list_score_configs()
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter_map(evaluators::definition_from_config)
            .collect::<Vec<_>>();
        let matching = existing
            .iter()
            .filter(|definition| {
                (definition.evaluator_id == evaluator.evaluator_id
                    || (definition.target_agent_name == evaluator.target_agent_name
                        && definition.name == evaluator.name))
                    && definition.target_agent_name == evaluator.target_agent_name
                    && definition.name == evaluator.name
                    && definition.criteria == evaluator.criteria
            })
            .max_by_key(|definition| {
                (definition.active, definition.timestamp.clone(), definition.version)
            });
        if let Some(existing) = matching {
            return Ok((StatusCode::OK, Json(existing.clone())));
        }
        if let Some(latest) = existing
            .iter()
            .filter(|definition| definition.evaluator_id == evaluator.evaluator_id)
            .max_by_key(|definition| definition.version)
        {
            evaluator.version = latest
                .version
                .checked_add(1)
                .ok_or_else(|| bad_request("evaluation version limit reached"))?;
        } else {
            evaluator.version = 1;
        }
    }
    let (definition, created) = evaluators::save_evaluator(&workspace, evaluator).await?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(definition),
    ))
}

pub async fn mcp_activate_evaluator(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<McpEvaluatorActivationRequest>,
) -> Result<Json<EvaluatorDefinition>, ApiError> {
    let auth = require_lisa_service(&headers)?;
    let workspace = state
        .workspace_for_auth(&auth)
        .await
        .map_err(storage_error)?;
    let (evaluator_id, version) = if request.version == 0 {
        let matches = workspace
            .query()
            .list_score_configs()
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter_map(evaluators::definition_from_config)
            .filter(|definition| {
                definition.evaluator_id == request.evaluator_id
                    || (definition.name == request.name
                        && definition.target_agent_name == request.target_agent_name)
            })
            .collect::<Vec<_>>();
        let criteria = matches
            .iter()
            .map(|definition| definition.criteria.as_str())
            .collect::<std::collections::HashSet<_>>();
        if criteria.len() > 1 {
            return Err(bad_request(
                "multiple evaluations have this name and agent; activate the intended rubric in Explorer",
            ));
        }
        let definition = matches
            .into_iter()
            .max_by_key(|definition| {
                (definition.active, definition.timestamp.clone(), definition.version)
            })
            .ok_or_else(|| bad_request("evaluation not found"))?;
        (definition.evaluator_id, definition.version)
    } else {
        (request.evaluator_id, request.version)
    };
    let definition = evaluators::set_activation_for_workspace(
        &workspace,
        evaluator_id,
        version,
        true,
    )
    .await?;
    Ok(Json(definition))
}

fn compact_session_evidence(detail: Value) -> Value {
    session_evidence(detail, SESSION_SPAN_LIMIT)
}

fn session_evidence(detail: Value, span_limit: usize) -> Value {
    let scores = detail
        .get("scores")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let all_spans = detail
        .get("spans")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let agent_names = all_spans
        .iter()
        .filter_map(|span| span.get("agent_name").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    let spans = all_spans
        .into_iter()
        .take(span_limit)
        .map(|span| {
            let mut evidence = serde_json::Map::new();
            for key in [
                "span_id",
                "trace_id",
                "parent_span_id",
                "name",
                "observation_type",
                "status_code",
                "model_name",
                "start_time",
                "end_time",
            ] {
                if let Some(value) = span.get(key) {
                    evidence.insert(key.to_string(), value.clone());
                }
            }
            for key in ["input", "output", "attributes", "events"] {
                if let Some(value) = span.get(key) {
                    evidence.insert(key.to_string(), truncate_value(value, 4_000));
                }
            }
            Value::Object(evidence)
        })
        .collect::<Vec<_>>();
    json!({
        "session_id": detail.get("session_id"),
        "agent_names": agent_names,
        "scores": scores.into_iter().take(20).collect::<Vec<_>>(),
        "spans": spans,
        "evidence_limited": detail
            .get("spans")
            .and_then(Value::as_array)
            .is_some_and(|spans| spans.len() > span_limit),
    })
}

fn truncate_value(value: &Value, limit: usize) -> Value {
    let rendered = value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string());
    if rendered.len() <= limit {
        return value.clone();
    }
    let mut end = limit;
    while !rendered.is_char_boundary(end) {
        end -= 1;
    }
    Value::String(format!("{}… [truncated]", &rendered[..end]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_session_search_narrows_summaries_before_loading_trace_evidence() {
        let sessions = vec![
            json!({"session_id":"support-2","agent_name":"support-agent","error_count":0}),
            json!({"session_id":"travel-1","agent_name":"travel-agent","error_count":1}),
            json!({"session_id":"support-1","agent_name":"support-agent","error_count":1}),
        ];

        let filtered = filter_session_summaries(sessions, "support-agent", true);

        assert_eq!(filtered, vec![json!({
            "session_id":"support-1","agent_name":"support-agent","error_count":1
        })]);
    }

    #[test]
    fn session_evidence_returns_authenticated_agent_names_for_scope_checks() {
        let evidence = session_evidence(
            json!({
                "session_id": "session-1",
                "spans": [
                    {"agent_name": "support-agent", "trace_id": "trace-1"},
                    {"agent_name": "support-agent", "trace_id": "trace-1"},
                    {"agent_name": "billing-agent", "trace_id": "trace-2"}
                ],
                "scores": []
            }),
            20,
        );
        assert_eq!(
            evidence["agent_names"],
            json!(["billing-agent", "support-agent"])
        );
    }

    #[test]
    fn mcp_service_token_is_verified_without_plain_string_comparison() {
        assert!(verify_service_token("service-secret", "service-secret"));
        assert!(!verify_service_token("service-secret", "wrong-secret"));
        assert!(!verify_service_token("", ""));
    }

    #[test]
    fn mcp_tool_requests_reject_unexpected_fields() {
        assert!(serde_json::from_value::<McpSessionSearchRequest>(json!({
            "query": "refund"
        }))
        .is_ok());
        assert!(serde_json::from_value::<McpSessionSearchRequest>(json!({
            "workspace_id": "11111111-1111-4111-8111-111111111111",
            "query": "refund"
        }))
        .is_err());
    }
}
