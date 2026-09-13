//! Softprobe edge identity assertion (`X-Softprobe-Assertion`, sp-llm#39).
//!
//! Explorer / Cloudflare gateways mint a short-lived HS256 JWT. thelake verifies
//! the signature and binds DuckLake scope from claim `tenant_key`.

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
    #[serde(default)]
    pub tenant_id: Option<i64>,
    /// Softprobe `tenants.tenant_id` string = DuckLake `scope_id`.
    #[serde(default)]
    pub tenant_key: Option<String>,
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

pub fn tenant_info_from_assertion(claims: &SoftprobeAssertionClaims) -> Result<TenantInfo> {
    let tenant_key = claims
        .tenant_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("assertion: tenant_key required"))?
        .to_string();
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
        tenant_id: tenant_key,
        // Bucket/dataset historically came from Softprobe auth resources.
        // DuckLake scope registry is authoritative for storage paths; these
        // fields are only required by ducklake-connection material.
        bucket_name: std::env::var("DATALAKE_BUCKET").unwrap_or_default(),
        dataset_id: String::new(),
        agent_id,
        agent_name,
    })
}

/// Optional DuckLake `scope_id` / `tenant_key` for legacy clients that send
/// Bearer auth but no `X-Softprobe-Assertion` (see docs/compat/auth.md).
///
/// Env: `SOFTPROBE_DEFAULT_TENANT_KEY` or alias `THELAKE_DEFAULT_TENANT_KEY`.
/// Reserved ops id `thelake-ops` is rejected.
pub fn default_tenant_key_from_env() -> Option<String> {
    for key in ["SOFTPROBE_DEFAULT_TENANT_KEY", "THELAKE_DEFAULT_TENANT_KEY"] {
        if let Ok(v) = std::env::var(key) {
            let t = v.trim();
            if t.is_empty() {
                continue;
            }
            if crate::self_monitoring::is_reserved_tenant_id(t) {
                tracing::warn!(
                    "{key}={t} is reserved; ignoring default-tenant fallback"
                );
                return None;
            }
            return Some(t.to_string());
        }
    }
    None
}

/// Bind legacy Bearer-only traffic to a configured default lake scope.
pub fn tenant_info_for_default_lake(tenant_key: &str) -> Result<TenantInfo> {
    let tenant_key = tenant_key.trim();
    if tenant_key.is_empty() {
        bail!("default tenant_key is empty");
    }
    if crate::self_monitoring::is_reserved_tenant_id(tenant_key) {
        bail!("default tenant_key must not be reserved ops scope");
    }
    Ok(TenantInfo {
        tenant_id: tenant_key.to_string(),
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
                "tenant_id": 105,
                "tenant_key": "sp-llm-gke-smoke",
                "roles": ["member"],
                "exp": now + 300
            }),
            "test-secret",
        );
        let claims = verify_softprobe_assertion(&token, "test-secret", now).expect("ok");
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.tenant_key.as_deref(), Some("sp-llm-gke-smoke"));
        let info = tenant_info_from_assertion(&claims).unwrap();
        assert_eq!(info.tenant_id, "sp-llm-gke-smoke");
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
                "tenant_key": "ws-a",
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
    fn rejects_missing_tenant_key_for_tenant_info() {
        let now = 1_700_000_000_i64;
        let token = mint(
            serde_json::json!({
                "iss": "softprobe-edge",
                "aud": "sp-backend",
                "sub": "user-1",
                "tenant_id": 105,
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
                "tenant_key": "ws-a",
                "exp": now + 300
            }),
            "test-secret",
        );
        assert!(verify_softprobe_assertion(&token, "other-secret", now).is_err());
    }

    #[test]
    fn default_tenant_key_from_env_reads_primary_and_alias() {
        let prev_primary = std::env::var("SOFTPROBE_DEFAULT_TENANT_KEY").ok();
        let prev_alias = std::env::var("THELAKE_DEFAULT_TENANT_KEY").ok();
        std::env::remove_var("SOFTPROBE_DEFAULT_TENANT_KEY");
        std::env::remove_var("THELAKE_DEFAULT_TENANT_KEY");
        assert_eq!(default_tenant_key_from_env(), None);

        std::env::set_var("THELAKE_DEFAULT_TENANT_KEY", "ws-myworkspace-mtyxusmz-2t77yn");
        assert_eq!(
            default_tenant_key_from_env().as_deref(),
            Some("ws-myworkspace-mtyxusmz-2t77yn")
        );

        std::env::set_var("SOFTPROBE_DEFAULT_TENANT_KEY", "ws-primary");
        assert_eq!(default_tenant_key_from_env().as_deref(), Some("ws-primary"));

        std::env::set_var("SOFTPROBE_DEFAULT_TENANT_KEY", "thelake-ops");
        assert_eq!(default_tenant_key_from_env(), None);

        match prev_primary {
            Some(v) => std::env::set_var("SOFTPROBE_DEFAULT_TENANT_KEY", v),
            None => std::env::remove_var("SOFTPROBE_DEFAULT_TENANT_KEY"),
        }
        match prev_alias {
            Some(v) => std::env::set_var("THELAKE_DEFAULT_TENANT_KEY", v),
            None => std::env::remove_var("THELAKE_DEFAULT_TENANT_KEY"),
        }
    }

    #[test]
    fn tenant_info_for_default_lake_binds_scope() {
        let info = tenant_info_for_default_lake("ws-myworkspace-mtyxusmz-2t77yn").unwrap();
        assert_eq!(info.tenant_id, "ws-myworkspace-mtyxusmz-2t77yn");
        assert!(info.agent_id.is_none());
        assert!(tenant_info_for_default_lake("thelake-ops").is_err());
        assert!(tenant_info_for_default_lake("  ").is_err());
    }
}
