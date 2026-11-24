//! Softprobe edge identity assertion (`X-Softprobe-Assertion`, sp-llm#39).
//!
//! Explorer / Cloudflare gateways mint a short-lived HS256 JWT. thelake verifies
//! the signature and binds DuckLake scope from claim `workspace_id`.

use crate::authn::WorkspaceAuth;
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const ASSERTION_HEADER: &str = "x-softprobe-assertion";
pub const DEFAULT_ISS: &str = "softprobe-edge";
pub const DEFAULT_AUD: &str = "sp-backend";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Deserialize)]
pub struct SoftprobeAssertionClaims {
    pub iss: String,
    pub aud: Aud,
    pub sub: String,
    /// Softprobe `workspaces.id` UUID string = DuckLake logical workspace.
    pub workspace_id: String,
    #[serde(default)]
    pub roles: Option<Vec<String>>,
    /// Capabilities granted to a machine assertion by its trusted issuer.
    #[serde(default)]
    pub capabilities: Option<Vec<String>>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub exp: Option<i64>,
    /// Softprobe agent id when the assertion was minted for an Ingestion Key.
    #[serde(default)]
    pub agent_id: Option<String>,
    /// Softprobe agent display name when minted for an Ingestion Key.
    #[serde(default)]
    pub agent_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Aud {
    One(String),
    Many(Vec<String>),
}

impl Aud {
    fn includes(&self, expected: &str) -> bool {
        match self {
            Aud::One(s) => s == expected,
            Aud::Many(v) => v.iter().any(|s| s == expected),
        }
    }
}

/// Shared HMAC secret for assertion verify (Explorer `ASSERTION_HMAC_SECRET`).
pub fn assertion_hmac_secret() -> Option<String> {
    for key in ["SOFTPROBE_ASSERTION_HMAC_SECRET", "ASSERTION_HMAC_SECRET"] {
        if let Ok(v) = std::env::var(key) {
            let t = v.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

pub fn ingestion_key_resolve_url() -> Option<String> {
    if let Ok(raw) = std::env::var("INGESTION_KEY_RESOLVE_URL") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Ok(base) = std::env::var("SOFTPROBE_EXPLORER_URL") {
        let trimmed = base.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            return Some(format!("{trimmed}/api/internal/ingestion-key/resolve"));
        }
    }
    None
}

pub fn is_ingestion_key_resolve_configured() -> bool {
    ingestion_key_resolve_url().is_some() && assertion_hmac_secret().is_some()
}

pub fn mint_resolve_ingestion_key_assertion(secret: &str, now_sec: i64) -> Result<String> {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = serde_json::json!({
        "iss": "softprobe-thelake",
        "aud": "sp-explorer",
        "sub": "thelake-server",
        "workspace_id": "00000000-0000-0000-0000-000000000000",
        "capabilities": ["resolve_ingestion_key"],
        "exp": now_sec + 60,
    });
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header}.{payload_b64}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("assertion: invalid hmac key"))?;
    mac.update(signing_input.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{sig}"))
}

#[derive(Debug, Clone)]
pub enum IngestionKeyResolveError {
    Unconfigured,
    Unauthorized,
    Upstream(String),
}

static INGESTION_KEY_CACHE: std::sync::OnceLock<
    dashmap::DashMap<[u8; 32], (SoftprobeAssertionClaims, std::time::Instant)>,
> = std::sync::OnceLock::new();
static INGESTION_KEY_HTTP_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

fn ingestion_key_cache(
) -> &'static dashmap::DashMap<[u8; 32], (SoftprobeAssertionClaims, std::time::Instant)> {
    INGESTION_KEY_CACHE.get_or_init(dashmap::DashMap::new)
}

fn ingestion_key_http_client() -> &'static reqwest::Client {
    INGESTION_KEY_HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

#[derive(Deserialize)]
struct IngestionKeyResolveResponse {
    assertion: String,
}

pub async fn resolve_ingestion_key(
    token: &str,
) -> std::result::Result<SoftprobeAssertionClaims, IngestionKeyResolveError> {
    let resolve_url = ingestion_key_resolve_url().ok_or(IngestionKeyResolveError::Unconfigured)?;
    let secret = assertion_hmac_secret().ok_or(IngestionKeyResolveError::Unconfigured)?;

    let cache_key: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let now_instant = std::time::Instant::now();
    let now_sec = chrono::Utc::now().timestamp();
    if let Some(entry) = ingestion_key_cache().get(&cache_key) {
        if now_instant < entry.1 && entry.0.exp.is_none_or(|exp| exp >= now_sec) {
            return Ok(entry.0.clone());
        }
    }

    let service_jwt = mint_resolve_ingestion_key_assertion(&secret, now_sec)
        .map_err(|e| IngestionKeyResolveError::Upstream(e.to_string()))?;

    let resp = ingestion_key_http_client()
        .post(&resolve_url)
        .header(ASSERTION_HEADER, service_jwt)
        .json(&serde_json::json!({ "ingestionKey": token }))
        .send()
        .await
        .map_err(|e| {
            IngestionKeyResolveError::Upstream(format!("ingestion key resolution failed: {e}"))
        })?;

    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Err(IngestionKeyResolveError::Unauthorized);
    }
    if !status.is_success() {
        return Err(IngestionKeyResolveError::Upstream(format!(
            "ingestion key resolution returned status {status}"
        )));
    }

    let payload: IngestionKeyResolveResponse = resp
        .json()
        .await
        .map_err(|_| IngestionKeyResolveError::Unauthorized)?;
    if payload.assertion.trim().is_empty() {
        return Err(IngestionKeyResolveError::Unauthorized);
    }

    let claims = verify_softprobe_assertion(&payload.assertion, &secret, now_sec)
        .map_err(|_| IngestionKeyResolveError::Unauthorized)?;

    let mut ttl = std::time::Duration::from_secs(60);
    if let Some(exp) = claims.exp {
        let remaining = exp.saturating_sub(now_sec);
        if remaining <= 0 {
            return Ok(claims);
        }
        ttl = ttl.min(std::time::Duration::from_secs(remaining as u64));
    }
    if !ttl.is_zero() {
        ingestion_key_cache().insert(cache_key, (claims.clone(), std::time::Instant::now() + ttl));
    }

    Ok(claims)
}

pub fn verify_softprobe_assertion(
    token: &str,
    secret: &str,
    now_sec: i64,
) -> Result<SoftprobeAssertionClaims> {
    let parts: Vec<&str> = token.trim().split('.').collect();
    if parts.len() != 3 {
        bail!("assertion: malformed jwt");
    }
    let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    let header_json = URL_SAFE_NO_PAD
        .decode(header_b64)
        .map_err(|_| anyhow!("assertion: bad header encoding"))?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_json).map_err(|_| anyhow!("assertion: bad header json"))?;
    if header.get("alg").and_then(|v| v.as_str()) != Some("HS256") {
        bail!("assertion: unsupported alg");
    }

    let signing_input = format!("{header_b64}.{payload_b64}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("assertion: invalid hmac key"))?;
    mac.update(signing_input.as_bytes());
    let expected = mac.finalize().into_bytes();
    let got = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| anyhow!("assertion: bad signature encoding"))?;
    if expected.as_slice() != got.as_slice() {
        bail!("assertion: bad signature");
    }

    let payload_json = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| anyhow!("assertion: bad payload encoding"))?;
    let claims: SoftprobeAssertionClaims = serde_json::from_slice(&payload_json)
        .map_err(|_| anyhow!("assertion: bad payload json"))?;

    if claims.iss != DEFAULT_ISS {
        bail!("assertion: bad iss");
    }
    if !claims.aud.includes(DEFAULT_AUD) {
        bail!("assertion: bad aud");
    }
    if claims.sub.trim().is_empty() {
        bail!("assertion: empty sub");
    }
    if let Some(exp) = claims.exp {
        if exp < now_sec {
            bail!("assertion: expired");
        }
    }

    Ok(claims)
}

/// Parse and normalize a Softprobe `workspace_id` (must be a UUID).
///
/// Shared by assertion claims, default-workspace env binding, and Bearer
/// auth-service responses so all identity paths enforce the same contract.
pub fn parse_workspace_id(raw: &str) -> Result<String> {
    let workspace_id = raw.trim();
    if workspace_id.is_empty() {
        bail!("workspace_id required");
    }
    let parsed =
        uuid::Uuid::parse_str(workspace_id).map_err(|_| anyhow!("workspace_id must be a UUID"))?;
    Ok(parsed.to_string())
}

pub fn workspace_auth_from_assertion(claims: &SoftprobeAssertionClaims) -> Result<WorkspaceAuth> {
    let workspace_id =
        parse_workspace_id(&claims.workspace_id).map_err(|err| anyhow!("assertion: {err}"))?;
    let agent_id = claims
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let agent_name = claims
        .agent_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(WorkspaceAuth {
        workspace_id,
        // Bucket/dataset historically came from Softprobe auth resources.
        // DuckLake scope registry is authoritative for storage paths; these
        // fields are only required by ducklake-connection material.
        bucket_name: std::env::var("DATALAKE_BUCKET").unwrap_or_default(),
        dataset_id: String::new(),
        agent_id,
        agent_name,
    })
}

#[cfg(test)]
pub(crate) static AUTH_ENV_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    const WS: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn mint(payload: serde_json::Value, secret: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
        let signing = format!("{header}.{payload_b64}");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(signing.as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{signing}.{sig}")
    }

    #[test]
    fn verifies_valid_assertion() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "user-1",
                "workspace_id": WS,
                "roles": ["member"],
                "exp": now + 300
            }),
            "test-secret",
        );
        let claims = verify_softprobe_assertion(&token, "test-secret", now).expect("ok");
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.workspace_id, WS);
        let info = workspace_auth_from_assertion(&claims).unwrap();
        assert_eq!(info.workspace_id, WS);
        assert_eq!(info.agent_id, None);
        assert_eq!(info.agent_name, None);
    }

    #[test]
    fn carries_agent_claims_into_workspace_auth() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "agent-key",
                "workspace_id": WS,
                "agent_id": "support-refund-agent",
                "agent_name": "Support Refund Agent",
                "capabilities": ["ingest"],
                "exp": now + 300
            }),
            "test-secret",
        );
        let claims = verify_softprobe_assertion(&token, "test-secret", now).expect("ok");
        assert_eq!(claims.agent_id.as_deref(), Some("support-refund-agent"));
        assert_eq!(claims.agent_name.as_deref(), Some("Support Refund Agent"));
        assert_eq!(
            claims.capabilities.as_deref(),
            Some(&["ingest".to_string()][..])
        );
        let info = workspace_auth_from_assertion(&claims).unwrap();
        assert_eq!(info.agent_id.as_deref(), Some("support-refund-agent"));
        assert_eq!(info.agent_name.as_deref(), Some("Support Refund Agent"));
    }

    #[test]
    fn rejects_missing_workspace_id_for_workspace_auth() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "user-1",
                "exp": now + 300
            }),
            "test-secret",
        );
        assert!(verify_softprobe_assertion(&token, "test-secret", now).is_err());
    }

    #[test]
    fn rejects_non_uuid_workspace_id() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "user-1",
                "workspace_id": "ws-not-a-uuid",
                "exp": now + 300
            }),
            "test-secret",
        );
        let claims = verify_softprobe_assertion(&token, "test-secret", now).unwrap();
        assert!(workspace_auth_from_assertion(&claims).is_err());
    }

    #[test]
    fn rejects_bad_signature() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "user-1",
                "workspace_id": WS,
                "exp": now + 300
            }),
            "test-secret",
        );
        assert!(verify_softprobe_assertion(&token, "other-secret", now).is_err());
    }
}
