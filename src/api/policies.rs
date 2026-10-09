//! Workspace-scoped Markdown policy memory stored as immutable score-config
//! versions, so policy authoring does not create a second persistence system.

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

const MARKER: &str = "thelake.policy";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyDocument {
    pub policy_id: String,
    pub version: u32,
    /// `None` denotes the workspace-wide POLICY.md; otherwise this is an exact
    /// registered agent name.
    pub target_agent_name: Option<String>,
    pub content: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreatePolicyRequest {
    pub policy_id: String,
    pub version: u32,
    pub target_agent_name: Option<String>,
    pub content: String,
}

fn validate(request: &CreatePolicyRequest) -> Result<(), &'static str> {
    if request.policy_id.is_empty()
        || request.policy_id.len() > 200
        || !request
            .policy_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("policy_id must use 1-200 letters, digits, periods, underscores, or hyphens");
    }
    if request.version == 0 {
        return Err("version must be greater than zero");
    }
    if request
        .target_agent_name
        .as_ref()
        .is_some_and(|name| name.trim().is_empty() || name.len() > 256)
    {
        return Err("target_agent_name must be between 1 and 256 characters");
    }
    if request.content.trim().is_empty() || request.content.len() > 32_000 {
        return Err("policy content must be between 1 and 32000 bytes");
    }
    Ok(())
}

pub(crate) fn config_id(policy_id: &str, version: u32) -> String {
    format!("policy:{policy_id}:v{version}")
}

fn config_from_request(request: CreatePolicyRequest) -> ScoreConfig {
    let timestamp = Utc::now();
    let mut metadata = HashMap::from([
        (MARKER.into(), "true".into()),
        ("thelake.policy.id".into(), request.policy_id.clone()),
        ("thelake.policy.version".into(), request.version.to_string()),
        ("thelake.policy.content".into(), request.content),
        (
            "thelake.policy.target_agent_name".into(),
            request.target_agent_name.unwrap_or_default(),
        ),
    ]);
    metadata.insert("thelake.policy.contract".into(), "1".into());
    ScoreConfig {
        config_id: config_id(&request.policy_id, request.version),
        timestamp,
        name: format!("POLICY.md · {} · v{}", request.policy_id, request.version),
        data_type: ScoreDataType::Text,
        description: Some("Workspace-scoped Markdown policy memory".into()),
        min_value: None,
        max_value: None,
        categories: Vec::new(),
        author_id: None,
        metadata,
        workspace_id: None,
    }
}

pub(crate) fn document_from_config(config: ScoreConfig) -> Option<PolicyDocument> {
    if config.metadata.get(MARKER).map(String::as_str) != Some("true") {
        return None;
    }
    Some(PolicyDocument {
        policy_id: config.metadata.get("thelake.policy.id")?.clone(),
        version: config
            .metadata
            .get("thelake.policy.version")?
            .parse()
            .ok()?,
        target_agent_name: config
            .metadata
            .get("thelake.policy.target_agent_name")
            .filter(|name| !name.is_empty())
            .cloned(),
        content: config.metadata.get("thelake.policy.content")?.clone(),
        timestamp: config.timestamp,
    })
}

pub async fn list_policies(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
) -> Result<Json<Vec<PolicyDocument>>, ApiError> {
    let auth_info = auth.as_ref().map(|extension| &extension.0);
    let ws = match auth_info {
        Some(info) => state.workspace_for_auth(info).await,
        None => state.workspace_for_id("").await,
    }
    .map_err(|error| {
        warn!("failed to resolve workspace for policy list: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"workspace runtime unavailable"})),
        )
    })?;
    let configs = ws.query().list_score_configs().await.map_err(|error| {
        warn!("policy list failed: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"policy list failed"})),
        )
    })?;
    let mut documents = configs
        .into_iter()
        .filter_map(document_from_config)
        .collect::<Vec<_>>();
    documents.sort_by(|left, right| {
        left.policy_id
            .cmp(&right.policy_id)
            .then(left.version.cmp(&right.version))
    });
    Ok(Json(documents))
}

pub async fn create_policy(
    State(state): State<AppState>,
    auth: Option<Extension<WorkspaceAuth>>,
    Json(request): Json<CreatePolicyRequest>,
) -> Result<(StatusCode, Json<PolicyDocument>), ApiError> {
    validate(&request).map_err(|message| bad_request(message.to_string()))?;
    let auth_info = auth.as_ref().map(|extension| &extension.0);
    let ws = match auth_info {
        Some(info) => state.workspace_for_auth(info).await,
        None => state.workspace_for_id("").await,
    }
    .map_err(|error| {
        warn!("failed to resolve workspace for policy creation: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"workspace runtime unavailable"})),
        )
    })?;
    let (document, created) = save_policy(&ws, request).await?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(document),
    ))
}

async fn save_policy(
    ws: &crate::workspace::WorkspaceContext,
    request: CreatePolicyRequest,
) -> Result<(PolicyDocument, bool), ApiError> {
    validate(&request).map_err(|message| bad_request(message.to_string()))?;
    let lock_key = format!("{}:{}", ws.workspace_id(), request.policy_id);
    ws.with_score_config_write_lock(&lock_key, || async move {
        let config = config_from_request(request);
        if let Some(existing) = ws
            .query()
            .get_score_config(&config.config_id)
            .await
            .map_err(|error| {
                warn!("policy idempotency lookup failed: {error}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error":"policy lookup failed"})),
                )
            })?
        {
            let existing = document_from_config(existing).ok_or_else(|| {
                bad_request("policy_id and version already identify a non-policy config")
            })?;
            let requested = document_from_config(config).expect("request creates a valid policy");
            if existing.policy_id != requested.policy_id
                || existing.version != requested.version
                || existing.target_agent_name != requested.target_agent_name
                || existing.content != requested.content
            {
                return Err(bad_request(
                    "policy_id and version already identify different immutable content",
                ));
            }
            return Ok((existing, false));
        }
        ws.ingest()
            .add_score_configs(vec![config.clone()])
            .await
            .map_err(|error| {
                warn!("policy document write failed: {error}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error":"policy write failed"})),
                )
            })?;
        Ok((
            document_from_config(config).expect("new policy config is valid"),
            true,
        ))
    })
    .await
    .map_err(|error| {
        warn!("policy write lock failed: {error}");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"policy write lock unavailable"})),
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> CreatePolicyRequest {
        CreatePolicyRequest {
            policy_id: "workspace".into(),
            version: 1,
            target_agent_name: None,
            content: "# Workspace policy\n\n## Handle refunds\n- Status: candidate\n".into(),
        }
    }

    #[test]
    fn policy_document_round_trips_through_score_config() {
        let original = request();
        let config = config_from_request(original);
        let document = document_from_config(config).expect("policy document");
        assert_eq!(document.policy_id, "workspace");
        assert_eq!(document.version, 1);
        assert_eq!(document.target_agent_name, None);
        assert!(document.content.contains("Status: candidate"));
    }

    #[test]
    fn policy_documents_require_bounded_markdown_and_identity() {
        let mut invalid = request();
        invalid.policy_id = "bad/id".into();
        assert!(validate(&invalid).is_err());
        let mut invalid = request();
        invalid.content = " ".into();
        assert!(validate(&invalid).is_err());
    }
}
