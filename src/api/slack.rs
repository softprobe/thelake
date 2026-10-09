//! First-party Slack Events API integration for authoring behavior checks and
//! receiving online evaluation failures.

use crate::api::evaluators::{self, CreateEvaluatorRequest, EvaluatorDefinition};
use crate::api::AppState;
use crate::slack_events::SlackEventClaim;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use std::time::Duration;
use tracing::warn;

type SlackHmac = Hmac<Sha256>;

#[derive(Debug, Deserialize)]
struct SlackEnvelope {
    #[serde(rename = "type")]
    kind: String,
    challenge: Option<String>,
    event_id: Option<String>,
    team_id: Option<String>,
    event: Option<SlackEvent>,
}

#[derive(Debug, Deserialize)]
struct SlackEvent {
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
    channel: Option<String>,
    ts: Option<String>,
    thread_ts: Option<String>,
    subtype: Option<String>,
    #[serde(rename = "user")]
    user_id: Option<String>,
}

#[derive(Debug)]
struct RuleCommand {
    target_agent_name: String,
    criteria: String,
}

#[derive(Serialize)]
struct SlackMessage<'a> {
    channel: &'a str,
    thread_ts: &'a str,
    text: String,
}

pub async fn events(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, StatusCode> {
    let secret = std::env::var("THELAKE_SLACK_SIGNING_SECRET")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let timestamp = headers
        .get("x-slack-request-timestamp")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let signature = headers
        .get("x-slack-signature")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let now = chrono::Utc::now().timestamp();
    if !verify_signature(&secret, timestamp, signature, &body, now) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let envelope: SlackEnvelope =
        serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    if envelope.kind == "url_verification" {
        return Ok(Json(
            json!({ "challenge": envelope.challenge.unwrap_or_default() }),
        ));
    }
    if envelope.kind != "event_callback" {
        return Ok(Json(json!({ "ok": true })));
    }
    let expected_team = std::env::var("THELAKE_SLACK_TEAM_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    if !slack_team_matches(&expected_team, envelope.team_id.as_deref()) {
        return Err(StatusCode::FORBIDDEN);
    }

    let Some(event_id) = envelope.event_id else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let Some(event) = envelope.event else {
        return Ok(Json(json!({ "ok": true })));
    };
    let eligible = event.kind == "app_mention"
        || (event.kind == "message" && event.thread_ts.is_some() && event.subtype.is_none());
    if !eligible {
        return Ok(Json(json!({ "ok": true })));
    }
    let Some(command) = event.text.as_deref().and_then(parse_rule_command) else {
        return Ok(Json(json!({ "ok": true })));
    };
    let (Some(channel), Some(message_ts)) = (event.channel, event.ts) else {
        return Ok(Json(json!({ "ok": true })));
    };
    let allowed_users = std::env::var("THELAKE_SLACK_ALLOWED_USERS").ok();
    let allowed_channels = std::env::var("THELAKE_SLACK_ALLOWED_CHANNELS").ok();
    if !has_authorization_policy(allowed_users.as_deref(), allowed_channels.as_deref()) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    if !author_allowed(
        allowed_users.as_deref(),
        allowed_channels.as_deref(),
        event.user_id.as_deref(),
        &channel,
    ) {
        return Err(StatusCode::FORBIDDEN);
    }
    let thread_ts = event.thread_ts.unwrap_or(message_ts);

    let workspace_id = resolve_slack_workspace_id(
        std::env::var("THELAKE_SLACK_WORKSPACE_ID").ok().as_deref(),
        std::env::var("THELAKE_DEFAULT_WORKSPACE_ID")
            .ok()
            .as_deref(),
        state.workspaces.config().ducklake.workspace_scope_mode
            == crate::workspace_scope::WorkspaceScopeMode::Shared,
    )
    .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let event_store = state.workspaces.slack_event_store();
    let claim_id = match event_store
        .claim(&expected_team, &event_id)
        .await
        .map_err(|error| {
            warn!("Slack event receipt claim failed: {error}");
            StatusCode::SERVICE_UNAVAILABLE
        })? {
        SlackEventClaim::Complete => return Ok(Json(json!({ "ok": true }))),
        SlackEventClaim::InProgress => return Err(StatusCode::SERVICE_UNAVAILABLE),
        SlackEventClaim::Claimed(claim_id) => claim_id,
    };
    // Keep handler work comfortably inside the receipt lease. If storage or
    // Slack is slow, cancel this attempt and let Slack retry the event.
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        create_rule_from_slack(
            state,
            &workspace_id,
            &channel,
            &thread_ts,
            &event_id,
            command,
        ),
    )
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("Slack evaluator authoring timed out")));
    if let Err(error) = result {
        if let Err(release_error) = event_store
            .release(&expected_team, &event_id, &claim_id)
            .await
        {
            warn!("Slack event receipt release failed: {release_error}");
        }
        warn!("Slack evaluator authoring failed: {error}");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    event_store
        .complete(&expected_team, &event_id, &claim_id)
        .await
        .map_err(|error| {
            warn!("Slack event receipt completion failed: {error}");
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    Ok(Json(json!({ "ok": true })))
}

async fn create_rule_from_slack(
    state: AppState,
    workspace_id: &str,
    channel: &str,
    thread_ts: &str,
    event_id: &str,
    command: RuleCommand,
) -> anyhow::Result<()> {
    let ws = state.workspaces.workspace_for(workspace_id).await?;
    let evaluator_id = evaluator_id(event_id);
    let name = command
        .criteria
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    let request = CreateEvaluatorRequest {
        evaluator_id,
        version: 1,
        target_agent_name: command.target_agent_name,
        name: format!("Behavior: {name}"),
        criteria: command.criteria,
        threshold: 0.7,
        uncertainty_margin: 0.1,
        required_tool_order: Vec::new(),
        slack_channel_id: Some(channel.to_string()),
        slack_thread_ts: Some(thread_ts.to_string()),
    };
    let (definition, _) = evaluators::save_evaluator(&ws, request)
        .await
        .map_err(|(status, _)| anyhow::anyhow!("evaluator API returned {status}"))?;
    evaluators::set_activation_for_workspace(
        &ws,
        definition.evaluator_id.clone(),
        definition.version,
        true,
    )
    .await
    .map_err(|(status, _)| anyhow::anyhow!("evaluator activation returned {status}"))?;
    let safe_agent = safe_slack_text(&definition.target_agent_name);
    let safe_name = safe_slack_text(&definition.name);
    post_message(
        channel,
        thread_ts,
        &format!("Behavior check created and activated for `{safe_agent}`: {safe_name}"),
    )
    .await?;
    Ok(())
}

/// Send a failure notification after the evaluator score has been persisted.
/// Delivery is best effort; score storage remains the source of truth.
pub(crate) async fn notify_evaluator_failure(
    definition: &EvaluatorDefinition,
    trace_id: &str,
    rationale: &str,
) -> anyhow::Result<()> {
    let (Some(channel), Some(thread_ts)) = (
        definition.slack_channel_id.as_deref(),
        definition.slack_thread_ts.as_deref(),
    ) else {
        return Ok(());
    };
    post_message(
        channel,
        thread_ts,
        &format!(
            ":warning: *Behavior check failed:* {}\nAgent: `{}` · trace: `{}`\n{}",
            safe_slack_text(&definition.name),
            safe_slack_text(&definition.target_agent_name),
            safe_slack_text(trace_id),
            safe_slack_text(rationale)
        ),
    )
    .await
}

async fn post_message(channel: &str, thread_ts: &str, text: &str) -> anyhow::Result<()> {
    let token = std::env::var("THELAKE_SLACK_BOT_TOKEN")
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("Slack bot token is not configured"))?;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(token)
        .json(&SlackMessage {
            channel,
            thread_ts,
            text: text.to_string(),
        })
        .send()
        .await?;
    let status = response.status();
    let result: SlackApiResponse = response.json().await?;
    if !status.is_success() || !result.ok {
        anyhow::bail!("Slack chat.postMessage failed");
    }
    Ok(())
}

#[derive(Deserialize)]
struct SlackApiResponse {
    ok: bool,
}

fn evaluator_id(event_id: &str) -> String {
    use sha2::Digest;
    let digest = Sha256::digest(event_id.as_bytes());
    format!("slack-{}", hex::encode(&digest[..12]))
}

fn resolve_slack_workspace_id(
    slack_workspace: Option<&str>,
    default_workspace: Option<&str>,
    shared_scope: bool,
) -> Option<String> {
    slack_workspace
        .or(default_workspace)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| shared_scope.then(String::new))
}

fn slack_team_matches(expected: &str, actual: Option<&str>) -> bool {
    actual == Some(expected)
}

fn has_authorization_policy(users: Option<&str>, channels: Option<&str>) -> bool {
    [users, channels]
        .into_iter()
        .flatten()
        .flat_map(|values| values.split(','))
        .any(|value| !value.trim().is_empty())
}

fn author_allowed(
    users: Option<&str>,
    channels: Option<&str>,
    user_id: Option<&str>,
    channel_id: &str,
) -> bool {
    let user_is_allowed = user_id.is_some_and(|user_id| {
        users
            .into_iter()
            .flat_map(|values| values.split(','))
            .any(|value| !value.trim().is_empty() && value.trim() == user_id)
    });
    let channel_is_allowed = channels
        .into_iter()
        .flat_map(|values| values.split(','))
        .any(|value| !value.trim().is_empty() && value.trim() == channel_id);
    user_is_allowed || channel_is_allowed
}

fn parse_rule_command(text: &str) -> Option<RuleCommand> {
    let cleaned = text
        .split_whitespace()
        .filter(|word| !(word.starts_with("<@") && word.ends_with('>')))
        .collect::<Vec<_>>()
        .join(" ");
    let command = cleaned.strip_prefix("evaluate ")?;
    let (target_agent_name, criteria) = command.split_once(" :: ")?;
    let target_agent_name = target_agent_name.trim();
    let criteria = criteria.trim();
    if target_agent_name.is_empty() || target_agent_name.len() > 256 || criteria.is_empty() {
        return None;
    }
    Some(RuleCommand {
        target_agent_name: target_agent_name.to_string(),
        criteria: criteria.to_string(),
    })
}

fn verify_signature(secret: &str, timestamp: &str, signature: &str, body: &[u8], now: i64) -> bool {
    let Ok(timestamp_number) = timestamp.parse::<i64>() else {
        return false;
    };
    if now.abs_diff(timestamp_number) > 300 {
        return false;
    }
    let Some(hex_signature) = signature.strip_prefix("v0=") else {
        return false;
    };
    let Ok(signature_bytes) = hex::decode(hex_signature) else {
        return false;
    };
    let Ok(mut mac) = SlackHmac::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(format!("v0:{timestamp}:").as_bytes());
    mac.update(body);
    mac.verify_slice(&signature_bytes).is_ok()
}

fn safe_slack_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('`', "'")
        .chars()
        .take(1200)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Request};
    use tower::ServiceExt;

    #[test]
    fn parses_thread_evaluator_command() {
        let command = parse_rule_command(
            "<@U123> evaluate support-agent :: The agent checks eligibility before refunding.",
        )
        .expect("command");
        assert_eq!(command.target_agent_name, "support-agent");
        assert_eq!(
            command.criteria,
            "The agent checks eligibility before refunding."
        );
    }

    #[test]
    fn rejects_incomplete_evaluator_command() {
        assert!(parse_rule_command("evaluate support-agent :: ").is_none());
        assert!(parse_rule_command("hello there").is_none());
    }

    #[test]
    fn escapes_slack_control_markup_in_evaluator_rationale() {
        assert_eq!(
            safe_slack_text("<@U123> `ping` & <all>"),
            "&lt;@U123&gt; 'ping' &amp; &lt;all&gt;"
        );
    }

    #[test]
    fn isolated_slack_workspace_fails_closed_without_binding() {
        assert_eq!(resolve_slack_workspace_id(None, None, false), None);
        assert_eq!(
            resolve_slack_workspace_id(None, Some("workspace-1"), false),
            Some("workspace-1".to_string())
        );
        assert_eq!(
            resolve_slack_workspace_id(None, None, true),
            Some(String::new())
        );
        assert!(slack_team_matches("team-a", Some("team-a")));
        assert!(!slack_team_matches("team-a", Some("team-b")));
    }

    #[test]
    fn evaluator_authoring_requires_allowlisted_user_or_channel() {
        assert!(!has_authorization_policy(None, None));
        assert!(has_authorization_policy(Some(" U1, "), None));
        assert!(author_allowed(Some("U1,U2"), None, Some("U2"), "C9"));
        assert!(author_allowed(None, Some("C8,C9"), None, "C9"));
        assert!(!author_allowed(Some("U1"), Some("C8"), Some("U2"), "C9"));
    }

    #[test]
    fn slack_event_ids_are_stable_and_extreme_signature_timestamps_reject() {
        assert_eq!(evaluator_id("event-1"), evaluator_id("event-1"));
        assert_ne!(evaluator_id("event-1"), evaluator_id("event-2"));
        assert!(!verify_signature(
            "secret",
            &i64::MIN.to_string(),
            "v0=00",
            b"",
            0
        ));
    }

    #[test]
    fn validates_slack_request_signature_and_timestamp_window() {
        let body = br#"{"type":"url_verification","challenge":"abc"}"#;
        let timestamp = "1791480000";
        let secret = "test-signing-secret";
        let signature = test_signature(secret, timestamp, body);
        assert!(verify_signature(
            secret,
            timestamp,
            &signature,
            body,
            1_791_480_000
        ));
        assert!(!verify_signature(
            secret,
            timestamp,
            &signature,
            body,
            1_791_480_301
        ));
        assert!(!verify_signature(
            secret,
            timestamp,
            "v0=bad",
            body,
            1_791_480_000
        ));
    }

    #[tokio::test]
    async fn signed_url_verification_returns_challenge() {
        let secret = "slack-signature-test";
        std::env::set_var("THELAKE_SLACK_SIGNING_SECRET", secret);
        let (router, _state, _temp_dir) = crate::test_support::local_router_and_state()
            .await
            .expect("router");
        let body = br#"{"type":"url_verification","challenge":"verify-me"}"#;
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let signature = test_signature(secret, &timestamp, body);
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/slack/events")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-slack-request-timestamp", timestamp)
                    .header("x-slack-signature", signature)
                    .body(Body::from(body.to_vec()))
                    .unwrap(),
            )
            .await
            .expect("Slack event response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: Value = serde_json::from_slice(&bytes).expect("JSON response");
        assert_eq!(payload["challenge"], "verify-me");
        std::env::remove_var("THELAKE_SLACK_SIGNING_SECRET");
    }

    fn test_signature(secret: &str, timestamp: &str, body: &[u8]) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("v0:{timestamp}:").as_bytes());
        mac.update(body);
        format!("v0={}", hex::encode(mac.finalize().into_bytes()))
    }
}
