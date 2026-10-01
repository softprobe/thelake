//! Softprobe edge identity assertion (`X-Softprobe-Assertion`, sp-llm#39).
//!
//! Explorer / Cloudflare gateways mint a short-lived HS256 JWT. thelake verifies
//! the signature and binds DuckLake scope from claim `workspace_id`.

use crate::authn::TenantInfo;
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

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
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub exp: Option<i64>,
    /// Softprobe agent id when the assertion was minted for an agent API key.
    #[serde(default)]
    pub agent_id: Option<String>,
    /// Softprobe agent display name when minted for an agent API key.
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
    let parsed = uuid::Uuid::parse_str(workspace_id)
        .map_err(|_| anyhow!("workspace_id must be a UUID"))?;
    Ok(parsed.to_string())
}

pub fn tenant_info_from_assertion(claims: &SoftprobeAssertionClaims) -> Result<TenantInfo> {
    let workspace_id = parse_workspace_id(&claims.workspace_id)
        .map_err(|err| anyhow!("assertion: {err}"))?;
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
    Ok(TenantInfo {
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

/// Optional DuckLake workspace id for clients that send Bearer auth but no
/// `X-Softprobe-Assertion` (see docs/compat/auth.md).
///
/// Env: `SOFTPROBE_DEFAULT_WORKSPACE_ID` or alias `THELAKE_DEFAULT_WORKSPACE_ID`.
/// Reserved ops id `thelake-ops` is rejected.
pub fn default_workspace_id_from_env() -> Option<String> {
    for key in [
        "SOFTPROBE_DEFAULT_WORKSPACE_ID",
        "THELAKE_DEFAULT_WORKSPACE_ID",
    ] {
        if let Ok(v) = std::env::var(key) {
            let t = v.trim();
            if t.is_empty() {
                continue;
            }
            if crate::self_monitoring::is_reserved_workspace_id(t) {
                tracing::warn!("{key}={t} is reserved; ignoring default-workspace fallback");
                return None;
            }
            match parse_workspace_id(t) {
                Ok(id) => return Some(id),
                Err(_) => {
                    tracing::warn!("{key}={t} is not a UUID; ignoring default-workspace fallback");
                    return None;
                }
            }
        }
    }
    None
}

/// Bind Bearer-only traffic to a configured default workspace.
pub fn tenant_info_for_default_lake(workspace_id: &str) -> Result<TenantInfo> {
    let workspace_id = parse_workspace_id(workspace_id)
        .map_err(|err| anyhow!("default workspace_id: {err}"))?;
    if crate::self_monitoring::is_reserved_workspace_id(&workspace_id) {
        bail!("default workspace_id must not be reserved ops scope");
    }
    Ok(TenantInfo {
        workspace_id,
        bucket_name: std::env::var("DATALAKE_BUCKET").unwrap_or_default(),
        dataset_id: String::new(),
        agent_id: None,
        agent_name: None,
    })
}

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
        let info = tenant_info_from_assertion(&claims).unwrap();
        assert_eq!(info.workspace_id, WS);
        assert_eq!(info.agent_id, None);
        assert_eq!(info.agent_name, None);
    }

    #[test]
    fn carries_agent_claims_into_tenant_info() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "agent-key",
                "workspace_id": WS,
                "agent_id": "support-refund-agent",
                "agent_name": "Support Refund Agent",
                "exp": now + 300
            }),
            "test-secret",
        );
        let claims = verify_softprobe_assertion(&token, "test-secret", now).expect("ok");
        assert_eq!(claims.agent_id.as_deref(), Some("support-refund-agent"));
        assert_eq!(claims.agent_name.as_deref(), Some("Support Refund Agent"));
        let info = tenant_info_from_assertion(&claims).unwrap();
        assert_eq!(info.agent_id.as_deref(), Some("support-refund-agent"));
        assert_eq!(info.agent_name.as_deref(), Some("Support Refund Agent"));
    }

    #[test]
    fn rejects_missing_workspace_id_for_tenant_info() {
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
        assert!(tenant_info_from_assertion(&claims).is_err());
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

    #[test]
    fn default_workspace_id_from_env_reads_primary_and_alias() {
        let prev_primary = std::env::var("SOFTPROBE_DEFAULT_WORKSPACE_ID").ok();
        let prev_alias = std::env::var("THELAKE_DEFAULT_WORKSPACE_ID").ok();
        std::env::remove_var("SOFTPROBE_DEFAULT_WORKSPACE_ID");
        std::env::remove_var("THELAKE_DEFAULT_WORKSPACE_ID");
        assert_eq!(default_workspace_id_from_env(), None);

        std::env::set_var("THELAKE_DEFAULT_WORKSPACE_ID", WS);
        assert_eq!(default_workspace_id_from_env().as_deref(), Some(WS));

        let primary = "11111111-1111-1111-1111-111111111111";
        std::env::set_var("SOFTPROBE_DEFAULT_WORKSPACE_ID", primary);
        assert_eq!(default_workspace_id_from_env().as_deref(), Some(primary));

        std::env::set_var("SOFTPROBE_DEFAULT_WORKSPACE_ID", "thelake-ops");
        assert_eq!(default_workspace_id_from_env(), None);

        std::env::set_var("SOFTPROBE_DEFAULT_WORKSPACE_ID", "not-a-uuid");
        assert_eq!(default_workspace_id_from_env(), None);

        match prev_primary {
            Some(v) => std::env::set_var("SOFTPROBE_DEFAULT_WORKSPACE_ID", v),
            None => std::env::remove_var("SOFTPROBE_DEFAULT_WORKSPACE_ID"),
        }
        match prev_alias {
            Some(v) => std::env::set_var("THELAKE_DEFAULT_WORKSPACE_ID", v),
            None => std::env::remove_var("THELAKE_DEFAULT_WORKSPACE_ID"),
        }
    }

    #[test]
    fn tenant_info_for_default_lake_binds_scope() {
        let info = tenant_info_for_default_lake(WS).unwrap();
        assert_eq!(info.workspace_id, WS);
        assert!(info.agent_id.is_none());
        assert!(tenant_info_for_default_lake("thelake-ops").is_err());
        assert!(tenant_info_for_default_lake("  ").is_err());
        assert!(tenant_info_for_default_lake("ws-slug").is_err());
    }
}
