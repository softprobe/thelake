//! Versioned, no-code behavior evaluator definitions.
//!
//! Definitions reuse the existing workspace-scoped score-config store. The
//! evaluator contract is kept in metadata so evaluator authoring does not
//! introduce a second, disconnected configuration registry.

use crate::api::error::{bad_request, ApiError};
use crate::api::AppState;
use crate::authn::WorkspaceAuth;
use crate::models::{ScoreConfig, ScoreDataType};
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

const MARKER: &str = "thelake.evaluator";
const ACTIVATION_MARKER: &str = "thelake.evaluator.activation";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedToolRequirement {
    pub before: String,
    pub action: String,
    #[serde(default = "default_true")]
    pub require_result_before_action: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluatorDefinition {
    pub evaluator_id: String,
    pub version: u32,
    pub target_agent_name: String,
    pub name: String,
    pub criteria: String,
    pub threshold: f64,
    pub uncertainty_margin: f64,
    #[serde(default)]
    pub required_tool_order: Vec<OrderedToolRequirement>,
    pub timestamp: DateTime<Utc>,
    pub active: bool,
    #[serde(skip)]
    pub slack_channel_id: Option<String>,
    #[serde(skip)]
    pub slack_thread_ts: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateEvaluatorRequest {
    pub evaluator_id: String,
    pub version: u32,
    pub target_agent_name: String,
    pub name: String,
    pub criteria: String,
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    #[serde(default = "default_margin")]
    pub uncertainty_margin: f64,
    #[serde(default)]
    pub required_tool_order: Vec<OrderedToolRequirement>,
    #[serde(skip)]
    pub slack_channel_id: Option<String>,
    #[serde(skip)]
    pub slack_thread_ts: Option<String>,
}

fn default_threshold() -> f64 {
    0.7
}

fn default_margin() -> f64 {
    0.1
}

fn validate(request: &CreateEvaluatorRequest) -> Result<(), &'static str> {
    if request.evaluator_id.is_empty()
        || request.evaluator_id.len() > 200
        || !request
            .evaluator_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(
            "evaluator_id must use 1-200 letters, digits, periods, underscores, or hyphens",
        );
    }
    if request.version == 0 {
        return Err("version must be greater than zero");
    }
    if request.target_agent_name.trim().is_empty() || request.target_agent_name.len() > 256 {
        return Err("target_agent_name must be between 1 and 256 characters");
    }
    if request.name.trim().is_empty() || request.name.len() > 200 {
        return Err("name must be between 1 and 200 characters");
    }
    if request.criteria.trim().is_empty() || request.criteria.len() > 8_000 {
        return Err("criteria must be between 1 and 8000 characters");
    }
    if !request.threshold.is_finite() || !(0.0..=1.0).contains(&request.threshold) {
        return Err("threshold must be between 0 and 1");
    }
    if !request.uncertainty_margin.is_finite()
        || !(0.0..=0.49).contains(&request.uncertainty_margin)
    {
        return Err("uncertainty_margin must be between 0 and 0.49");
    }
    if request.required_tool_order.len() > 20 {
        return Err("required_tool_order cannot contain more than 20 requirements");
    }
    if request.required_tool_order.iter().any(|rule| {
        rule.before.trim().is_empty()
            || rule.action.trim().is_empty()
            || rule.before.len() > 256
            || rule.action.len() > 256
    }) {
        return Err("tool names must be between 1 and 256 characters");
    }
    Ok(())
}

fn config_id(evaluator_id: &str, version: u32) -> String {
    format!("evaluator:{evaluator_id}:v{version}")
}

fn config_from_request(request: CreateEvaluatorRequest) -> ScoreConfig {
    let timestamp = Utc::now();
    let mut metadata = HashMap::from([
        (MARKER.to_string(), "true".to_string()),
        (
            "thelake.evaluator.id".to_string(),
            request.evaluator_id.clone(),
        ),
        (
            "thelake.evaluator.target_agent_name".to_string(),
            request.target_agent_name.clone(),
        ),
        (
            "thelake.evaluator.version".to_string(),
            request.version.to_string(),
        ),
        (
            "thelake.evaluator.criteria".to_string(),
            request.criteria.clone(),
        ),
        (
            "thelake.evaluator.threshold".to_string(),
            request.threshold.to_string(),
        ),
        (
            "thelake.evaluator.uncertainty_margin".to_string(),
            request.uncertainty_margin.to_string(),
        ),
        (
            "thelake.evaluator.required_tool_order".to_string(),
            serde_json::to_string(&request.required_tool_order).unwrap_or_else(|_| "[]".into()),
        ),
    ]);
    metadata.insert("thelake.evaluator.contract".into(), "1".into());
    if let Some(channel) = request.slack_channel_id {
        metadata.insert("thelake.evaluator.slack_channel_id".into(), channel);
    }
    if let Some(thread_ts) = request.slack_thread_ts {
        metadata.insert("thelake.evaluator.slack_thread_ts".into(), thread_ts);
    }
    ScoreConfig {
        config_id: config_id(&request.evaluator_id, request.version),
        timestamp,
        name: request.name,
        data_type: ScoreDataType::Categorical,
        description: Some(request.criteria),
        min_value: None,
        max_value: None,
        categories: vec![
            "pass".into(),
            "fail".into(),
            "uncertain".into(),
            "insufficient_evidence".into(),
            "error".into(),
        ],
        author_id: None,
        metadata,
        workspace_id: None,
    }
}

pub(crate) fn definition_from_config(config: ScoreConfig) -> Option<EvaluatorDefinition> {
    if config.metadata.get(MARKER).map(String::as_str) != Some("true") {
        return None;
    }
    let evaluator_id = config.metadata.get("thelake.evaluator.id")?.clone();
    let version = config
        .metadata
        .get("thelake.evaluator.version")?
        .parse()
        .ok()?;
    let target_agent_name = config
        .metadata
        .get("thelake.evaluator.target_agent_name")?
        .clone();
    let criteria = config.metadata.get("thelake.evaluator.criteria")?.clone();
    let threshold = config
        .metadata
        .get("thelake.evaluator.threshold")?
        .parse()
        .ok()?;
    let uncertainty_margin = config
        .metadata
        .get("thelake.evaluator.uncertainty_margin")?
        .parse()
        .ok()?;
    let required_tool_order = config
        .metadata
        .get("thelake.evaluator.required_tool_order")
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default();
    Some(EvaluatorDefinition {
        evaluator_id,
        version,
        target_agent_name,
        name: config.name,
        criteria,
        threshold,
        uncertainty_margin,
        required_tool_order,
        timestamp: config.timestamp,
        active: false,
        slack_channel_id: config
            .metadata
            .get("thelake.evaluator.slack_channel_id")
            .cloned(),
        slack_thread_ts: config
            .metadata
            .get("thelake.evaluator.slack_thread_ts")
            .cloned(),
    })
}

fn activation_config(evaluator_id: &str, version: u32, name: &str, enabled: bool) -> ScoreConfig {
    let timestamp = Utc::now();
    ScoreConfig {
        config_id: format!(
            "evaluator:activation:{evaluator_id}:{}",
            uuid::Uuid::new_v4()
        ),
        timestamp,
        name: format!("{name} activation"),
        data_type: ScoreDataType::Text,
        description: Some("Explicit activation record for an evaluator version".into()),
        min_value: None,
        max_value: None,
        categories: Vec::new(),
        author_id: None,
        metadata: HashMap::from([
            (ACTIVATION_MARKER.into(), "true".into()),
            ("thelake.evaluator.id".into(), evaluator_id.into()),
            ("thelake.evaluator.version".into(), version.to_string()),
            ("thelake.evaluator.enabled".into(), enabled.to_string()),
        ]),
        workspace_id: None,
    }
}

pub async fn create_evaluator(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<CreateEvaluatorRequest>,
) -> Result<(StatusCode, Json<EvaluatorDefinition>), ApiError> {
    validate(&request).map_err(|message| bad_request(message.to_string()))?;
    let auth_info = auth.as_ref().map(|extension| &extension.0);
    let ws = match auth_info {
        Some(info) => state.workspace_for_auth(info).await,
        None => state.workspace_for_id("").await,
    }
    .map_err(|error| {
        warn!("failed to resolve workspace for evaluator creation: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"workspace runtime unavailable"})),
        )
    })?;

    let (definition, created) = save_evaluator(&ws, request).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(definition)))
}

pub(crate) async fn save_evaluator(
    ws: &crate::workspace::WorkspaceContext,
    request: CreateEvaluatorRequest,
) -> Result<(EvaluatorDefinition, bool), ApiError> {
    validate(&request).map_err(|message| bad_request(message.to_string()))?;
    let config = config_from_request(request);
    if let Some(stored) = ws
        .query()
        .get_score_config(&config.config_id)
        .await
        .map_err(|error| {
            warn!("evaluator idempotency lookup failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":"evaluator lookup failed"})),
            )
        })?
    {
        let mut definition = definition_from_config(stored).ok_or_else(|| {
            bad_request("config_id is already used by a non-evaluator score config")
        })?;
        let requested = definition_from_config(config).expect("request creates a valid definition");
        if definition.target_agent_name != requested.target_agent_name
            || definition.name != requested.name
            || definition.criteria != requested.criteria
            || definition.threshold != requested.threshold
            || definition.uncertainty_margin != requested.uncertainty_margin
            || definition.slack_channel_id != requested.slack_channel_id
            || definition.slack_thread_ts != requested.slack_thread_ts
            || definition.required_tool_order.len() != requested.required_tool_order.len()
            || serde_json::to_value(&definition.required_tool_order).ok()
                != serde_json::to_value(&requested.required_tool_order).ok()
        {
            return Err(bad_request(
                "evaluator_id and version already identify a different immutable definition",
            ));
        }
        definition.active = is_version_active(ws, &definition.evaluator_id, definition.version)
            .await
            .map_err(|error| {
                warn!("evaluator activation lookup failed: {error}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error":"evaluator activation lookup failed"})),
                )
            })?;
        return Ok((definition, false));
    }

    ws.ingest()
        .add_score_configs(vec![config.clone()])
        .await
        .map_err(|error| {
            warn!("evaluator definition write failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":"evaluator write failed"})),
            )
        })?;
    let definition = definition_from_config(config).expect("new evaluator config is valid");
    Ok((definition, true))
}

pub async fn list_evaluators(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
) -> Result<Json<Vec<EvaluatorDefinition>>, ApiError> {
    let auth_info = auth.as_ref().map(|extension| &extension.0);
    let ws = match auth_info {
        Some(info) => state.workspace_for_auth(info).await,
        None => state.workspace_for_id("").await,
    }
    .map_err(|error| {
        warn!("failed to resolve workspace for evaluator list: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"workspace runtime unavailable"})),
        )
    })?;
    let configs = ws.query().list_score_configs().await.map_err(|error| {
        warn!("evaluator list failed: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"evaluator list failed"})),
        )
    })?;
    let active = configs
        .iter()
        .filter(|config| config.metadata.get(ACTIVATION_MARKER).map(String::as_str) == Some("true"))
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
            HashMap::<String, (u32, bool, DateTime<Utc>)>::new(),
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
    let mut definitions = configs
        .into_iter()
        .filter_map(definition_from_config)
        .collect::<Vec<_>>();
    for definition in &mut definitions {
        definition.active = active
            .get(&definition.evaluator_id)
            .is_some_and(|(version, enabled, _)| *version == definition.version && *enabled);
    }
    definitions.sort_by(|a, b| {
        a.evaluator_id
            .cmp(&b.evaluator_id)
            .then(a.version.cmp(&b.version))
    });
    Ok(Json(definitions))
}

pub async fn activate_evaluator(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
    axum::extract::Path((evaluator_id, version)): axum::extract::Path<(String, u32)>,
) -> Result<Json<EvaluatorDefinition>, ApiError> {
    set_evaluator_activation(State(state), auth, evaluator_id, version, true).await
}

pub async fn deactivate_evaluator(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
    axum::extract::Path((evaluator_id, version)): axum::extract::Path<(String, u32)>,
) -> Result<Json<EvaluatorDefinition>, ApiError> {
    set_evaluator_activation(State(state), auth, evaluator_id, version, false).await
}

async fn set_evaluator_activation(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
    evaluator_id: String,
    version: u32,
    enabled: bool,
) -> Result<Json<EvaluatorDefinition>, ApiError> {
    let auth_info = auth.as_ref().map(|extension| &extension.0);
    let ws = match auth_info {
        Some(info) => state.workspace_for_auth(info).await,
        None => state.workspace_for_id("").await,
    }
    .map_err(|error| {
        warn!("failed to resolve workspace for evaluator state change: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"workspace runtime unavailable"})),
        )
    })?;
    let definition = set_activation_for_workspace(&ws, evaluator_id, version, enabled).await?;
    Ok(Json(definition))
}

pub(crate) async fn set_activation_for_workspace(
    ws: &crate::workspace::WorkspaceContext,
    evaluator_id: String,
    version: u32,
    enabled: bool,
) -> Result<EvaluatorDefinition, ApiError> {
    if enabled
        && (std::env::var("THELAKE_EVALUATION_RUNNER_URL")
            .ok()
            .is_none_or(|value| value.trim().is_empty())
            || std::env::var("THELAKE_EVALUATION_RUNNER_TOKEN")
                .ok()
                .is_none_or(|value| value.trim().is_empty()))
    {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"online evaluation runner is not configured"})),
        ));
    }
    let Some(config) = ws
        .query()
        .get_score_config(&config_id(&evaluator_id, version))
        .await
        .map_err(|error| {
            warn!("evaluator lookup failed before activation: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":"evaluator lookup failed"})),
            )
        })?
    else {
        return Err(bad_request("evaluator version not found"));
    };
    let mut definition = definition_from_config(config.clone())
        .ok_or_else(|| bad_request("evaluator version not found"))?;
    if enabled
        && is_version_active(ws, &evaluator_id, version)
            .await
            .map_err(|error| {
                warn!("evaluator activation check failed: {error}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error":"evaluator activation check failed"})),
                )
            })?
    {
        definition.active = true;
        return Ok(definition);
    }
    let activation = activation_config(&evaluator_id, version, &definition.name, enabled);
    ws.ingest()
        .add_score_configs(vec![activation])
        .await
        .map_err(|error| {
            warn!("evaluator state write failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error":"evaluator state change failed"})),
            )
        })?;
    definition.active = enabled;
    Ok(definition)
}

async fn is_version_active(
    ws: &crate::workspace::WorkspaceContext,
    evaluator_id: &str,
    version: u32,
) -> anyhow::Result<bool> {
    let configs = ws.query().list_score_configs().await?;
    let latest = configs
        .iter()
        .filter(|config| {
            config.metadata.get(ACTIVATION_MARKER).map(String::as_str) == Some("true")
                && config
                    .metadata
                    .get("thelake.evaluator.id")
                    .map(String::as_str)
                    == Some(evaluator_id)
        })
        .max_by_key(|config| config.timestamp);
    Ok(latest.is_some_and(|config| {
        config
            .metadata
            .get("thelake.evaluator.version")
            .and_then(|value| value.parse::<u32>().ok())
            == Some(version)
            && config
                .metadata
                .get("thelake.evaluator.enabled")
                .map(String::as_str)
                == Some("true")
    }))
}
