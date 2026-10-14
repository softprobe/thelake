use crate::api::AppState;
use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::Response,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Prefer `X-Softprobe-Assertion`. Fall back to Bearer assertion JWT,
/// then optional `SOFTPROBE_DEFAULT_WORKSPACE_ID`, then Softprobe auth service.
pub async fn runtime_auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    if req.method() == Method::OPTIONS {
        return Ok(next.run(req).await);
    }

    let configured_service_token = std::env::var("LISA_THELAKE_TOKEN").unwrap_or_default();
    if let Some(auth) =
        lisa_service_auth_for_route(req.method(), path, req.headers(), &configured_service_token)?
    {
        req.extensions_mut().insert(auth);
        return Ok(next.run(req).await);
    }

    let local_anonymous_enabled = std::env::var("SOFTPROBE_LOCAL_ANONYMOUS").as_deref() == Ok("1");
    if local_anonymous_enabled && path.starts_with("/v1/") {
        let workspace_id = local_anonymous_workspace_id()?.ok_or(StatusCode::UNAUTHORIZED)?;
        if !is_local_anonymous_data_plane(req.method(), path) {
            return Err(StatusCode::FORBIDDEN);
        }
        let info = crate::softprobe_assertion::workspace_auth_for_default_lake(&workspace_id)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        req.extensions_mut().insert(info);
        return Ok(next.run(req).await);
    }
    if !requires_runtime_auth(req.method(), path) {
        return Ok(next.run(req).await);
    }

    if let Some(raw) = req
        .headers()
        .get(crate::softprobe_assertion::ASSERTION_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let secret =
            crate::softprobe_assertion::assertion_hmac_secret().ok_or(StatusCode::UNAUTHORIZED)?;
        let now = chrono::Utc::now().timestamp();
        let claims = crate::softprobe_assertion::verify_softprobe_assertion(raw, &secret, now)
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        let info = crate::softprobe_assertion::workspace_auth_from_assertion(&claims)
            .map_err(|_| StatusCode::FORBIDDEN)?;
        req.extensions_mut().insert(info);
        return Ok(next.run(req).await);
    }

    let auth = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = parse_bearer(auth).ok_or(StatusCode::UNAUTHORIZED)?;

    // Machine ingest keys may be long-lived Softprobe assertion JWTs (Explorer
    // workspace keys). Prefer verifying those before the Softprobe auth HTTP path
    // (which returns Softprobe numeric ids that are not DuckLake scope_ids).
    if token.chars().filter(|c| *c == '.').count() == 2 {
        if let Some(secret) = crate::softprobe_assertion::assertion_hmac_secret() {
            let now = chrono::Utc::now().timestamp();
            if let Ok(claims) =
                crate::softprobe_assertion::verify_softprobe_assertion(&token, &secret, now)
            {
                if let Ok(info) = crate::softprobe_assertion::workspace_auth_from_assertion(&claims)
                {
                    req.extensions_mut().insert(info);
                    return Ok(next.run(req).await);
                }
            }
        }
    }

    // Legacy direct-to-thelake clients: no assertion header. Optional default
    // lake routes them onto a configured MAP-ready scope (Bearer still required).
    if let Some(default_key) = crate::softprobe_assertion::default_workspace_id_from_env() {
        let info = crate::softprobe_assertion::workspace_auth_for_default_lake(&default_key)
            .map_err(|_| StatusCode::FORBIDDEN)?;
        req.extensions_mut().insert(info);
        return Ok(next.run(req).await);
    }

    let control_plane = state
        .workspaces
        .control_plane()
        .expect("runtime auth middleware requires control-plane state");
    let info = control_plane
        .resolver
        .resolve(&token)
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?;

    req.extensions_mut().insert(info);
    Ok(next.run(req).await)
}

fn authenticate_lisa_service(
    headers: &HeaderMap,
    configured_token: &str,
) -> Result<Option<crate::authn::WorkspaceAuth>, StatusCode> {
    const WORKSPACE_ID: &str = "x-thelake-workspace-id";
    const WORKSPACE_SIGNATURE: &str = "x-thelake-workspace-signature";
    let has_workspace_binding =
        headers.contains_key(WORKSPACE_ID) || headers.contains_key(WORKSPACE_SIGNATURE);
    let unauthorized = || Err(StatusCode::UNAUTHORIZED);

    let Some(authorization) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return if has_workspace_binding {
            unauthorized()
        } else {
            Ok(None)
        };
    };
    let Some(supplied_token) = parse_bearer(authorization) else {
        return if has_workspace_binding {
            unauthorized()
        } else {
            Ok(None)
        };
    };
    if !verify_lisa_service_token(configured_token, &supplied_token) {
        return if has_workspace_binding {
            unauthorized()
        } else {
            Ok(None)
        };
    }

    let workspace_id = headers
        .get(WORKSPACE_ID)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let workspace_id = crate::softprobe_assertion::parse_workspace_id(workspace_id)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let signature = headers
        .get(WORKSPACE_SIGNATURE)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !verify_lisa_workspace_binding(configured_token, &workspace_id, signature) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Some(crate::authn::WorkspaceAuth {
        workspace_id,
        bucket_name: String::new(),
        dataset_id: String::new(),
        agent_id: None,
        agent_name: None,
    }))
}

fn lisa_service_auth_for_route(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    configured_token: &str,
) -> Result<Option<crate::authn::WorkspaceAuth>, StatusCode> {
    let Some(auth) = authenticate_lisa_service(headers, configured_token)? else {
        return Ok(None);
    };
    if headers.contains_key(crate::softprobe_assertion::ASSERTION_HEADER) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if !is_lisa_service_route(method, path) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Some(auth))
}

fn verify_lisa_service_token(configured: &str, supplied: &str) -> bool {
    if configured.is_empty() || supplied.is_empty() {
        return false;
    }
    let Ok(mut expected_mac) = HmacSha256::new_from_slice(configured.as_bytes()) else {
        return false;
    };
    expected_mac.update(configured.as_bytes());
    let expected = expected_mac.finalize().into_bytes();
    let Ok(mut actual_mac) = HmacSha256::new_from_slice(configured.as_bytes()) else {
        return false;
    };
    actual_mac.update(supplied.as_bytes());
    actual_mac.verify_slice(&expected).is_ok()
}

fn verify_lisa_workspace_binding(token: &str, workspace_id: &str, signature: &str) -> bool {
    let Ok(signature) = hex::decode(signature) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(token.as_bytes()) else {
        return false;
    };
    mac.update(b"thelake-lisa-workspace-v1:");
    mac.update(workspace_id.as_bytes());
    mac.verify_slice(&signature).is_ok()
}

fn is_lisa_service_route(method: &Method, path: &str) -> bool {
    matches!(
        (method, path),
        (&Method::POST, "/v1/sessions/search") | (&Method::POST, "/v1/scores")
    ) || (*method == Method::GET && is_single_resource_path(path, "/v1/sessions/"))
}

/// Returns the configured local anonymous workspace, validating it whenever
/// the feature is enabled. Startup calls this once so bad configuration fails
/// before the listener accepts traffic; middleware calls it to bind requests.
pub fn local_anonymous_workspace_id() -> Result<Option<String>, StatusCode> {
    if std::env::var("SOFTPROBE_LOCAL_ANONYMOUS").as_deref() != Ok("1") {
        return Ok(None);
    }
    let raw = std::env::var("THELAKE_DEFAULT_WORKSPACE_ID")
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let workspace_id = crate::softprobe_assertion::parse_workspace_id(&raw)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if crate::self_monitoring::is_reserved_workspace_id(&workspace_id) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Some(workspace_id))
}

pub fn is_local_anonymous_data_plane(method: &Method, path: &str) -> bool {
    match (method, path) {
        (&Method::POST, "/v1/traces")
        | (&Method::POST, "/v1/logs")
        | (&Method::POST, "/v1/scores")
        | (&Method::POST, "/v1/spans/search")
        | (&Method::POST, "/v1/sessions/search")
        | (&Method::GET, "/v1/score-configs") => true,
        (&Method::GET, p) if is_single_resource_path(p, "/v1/spans/") => true,
        (&Method::GET, p) if is_single_resource_path(p, "/v1/traces/") => true,
        (&Method::GET, p) if is_single_resource_path(p, "/v1/sessions/") => true,
        _ => false,
    }
}

fn is_single_resource_path(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix)
        .is_some_and(|resource_id| !resource_id.is_empty() && !resource_id.contains('/'))
}

pub fn requires_runtime_auth(method: &Method, path: &str) -> bool {
    // Defense in depth with outermost CorsLayer: browser CORS preflight is OPTIONS
    // without Authorization. Auth here would 401 and block SPA OTLP
    // (e.g. @softprobe/web-record → POST /v1/traces).
    if *method == Method::OPTIONS && is_authenticated_api_prefix(path) {
        return false;
    }
    if path == "/v1/workspaces" && *method == Method::POST {
        return false;
    }
    is_authenticated_api_prefix(path)
}

/// Paths that require Bearer → tenant resolution (OTLP/control + compatibility stubs).
pub fn is_authenticated_api_prefix(path: &str) -> bool {
    path.starts_with("/v1/")
        || path.starts_with("/loki/api/v1/")
        || path.starts_with("/api/traces")
        || path.starts_with("/api/v2/traces")
        || path.starts_with("/api/search")
}

pub fn parse_bearer(h: &str) -> Option<String> {
    let h = h.trim();
    let rest = h.strip_prefix("Bearer ")?;
    let t = rest.trim();
    if t.is_empty() {
        return None;
    }
    Some(t.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn parses_valid_bearer_token() {
        assert_eq!(parse_bearer("Bearer abc").as_deref(), Some("abc"));
        assert_eq!(parse_bearer("Bearer  ").as_deref(), None);
    }

    #[test]
    fn trims_and_extracts_after_prefix() {
        assert_eq!(parse_bearer("  Bearer   tok  ").as_deref(), Some("tok"));
    }

    #[test]
    fn rejects_missing_or_empty_token() {
        assert!(parse_bearer("Bearer").is_none());
        assert!(parse_bearer("Bearer ").is_none());
        assert!(parse_bearer("").is_none());
        assert!(parse_bearer("Basic x").is_none());
    }

    #[test]
    fn skips_auth_for_v1_options_preflight() {
        assert!(!requires_runtime_auth(
            &Method::OPTIONS,
            "/v1/sessions/search"
        ));
        assert!(!requires_runtime_auth(&Method::OPTIONS, "/v1/traces"));
        assert!(!requires_runtime_auth(
            &Method::OPTIONS,
            "/loki/api/v1/labels"
        ));
        assert!(requires_runtime_auth(&Method::POST, "/v1/sessions/search"));
        assert!(requires_runtime_auth(&Method::POST, "/v1/traces"));
        assert!(requires_runtime_auth(&Method::GET, "/loki/api/v1/query"));
        assert!(requires_runtime_auth(&Method::GET, "/api/traces/abc"));
        assert!(requires_runtime_auth(&Method::GET, "/api/search"));
    }

    #[test]
    fn exempts_only_documented_non_v1_routes() {
        for path in ["/health", "/ready", "/openapi.json", "/swagger"] {
            assert!(
                !requires_runtime_auth(&Method::GET, path),
                "{path} must remain unauthenticated"
            );
        }

        for path in [
            "/v1/traces",
            "/v1/meta",
            "/v1/promotions/apply",
            "/loki/api/v1/labels",
            "/api/search/tags",
        ] {
            assert!(
                requires_runtime_auth(&Method::GET, path),
                "{path} must require auth"
            );
        }

        assert!(
            !requires_runtime_auth(&Method::POST, "/v1/workspaces"),
            "POST /v1/workspaces uses admin Bearer validated in-handler, not tenant middleware"
        );
    }

    #[test]
    fn reserved_ops_tenant_id_is_recognized() {
        assert!(crate::self_monitoring::is_reserved_workspace_id(
            "thelake-ops"
        ));
        assert!(!crate::self_monitoring::is_reserved_workspace_id(
            "softprobe-local"
        ));
    }

    #[test]
    fn local_anonymous_allowlist_excludes_control_plane() {
        assert!(is_local_anonymous_data_plane(&Method::POST, "/v1/traces"));
        assert!(is_local_anonymous_data_plane(
            &Method::POST,
            "/v1/sessions/search"
        ));
        assert!(is_local_anonymous_data_plane(&Method::POST, "/v1/scores"));
        assert!(!is_local_anonymous_data_plane(
            &Method::POST,
            "/v1/workspaces"
        ));
        assert!(!is_local_anonymous_data_plane(&Method::GET, "/v1/meta"));
        assert!(!is_local_anonymous_data_plane(
            &Method::POST,
            "/v1/promotions/apply"
        ));
        assert!(!is_local_anonymous_data_plane(
            &Method::POST,
            "/v1/score-configs"
        ));
        assert!(is_local_anonymous_data_plane(
            &Method::GET,
            "/v1/sessions/session-1"
        ));
        assert!(!is_local_anonymous_data_plane(
            &Method::GET,
            "/v1/sessions/session-1/recording"
        ));
    }

    #[test]
    fn lisa_service_auth_is_workspace_bound_and_rejects_invalid_binding() {
        let workspace_id = "550e8400-e29b-41d4-a716-446655440000";
        let mut headers = lisa_headers("service-secret", workspace_id);
        let auth = authenticate_lisa_service(&headers, "service-secret")
            .expect("valid credentials")
            .expect("Lisa service identity");
        assert_eq!(auth.workspace_id, workspace_id);

        headers.insert(
            "x-thelake-workspace-signature",
            "bad-signature".parse().unwrap(),
        );
        assert!(matches!(
            authenticate_lisa_service(&headers, "service-secret"),
            Err(StatusCode::UNAUTHORIZED)
        ));
    }

    #[test]
    fn lisa_service_credentials_cannot_escape_allowlist_or_mix_identities() {
        let mut headers = lisa_headers("service-secret", "550e8400-e29b-41d4-a716-446655440000");
        headers.insert(
            crate::softprobe_assertion::ASSERTION_HEADER,
            "another-identity".parse().unwrap(),
        );
        assert!(matches!(
            lisa_service_auth_for_route(
                &Method::POST,
                "/v1/sessions/search",
                &headers,
                "service-secret"
            ),
            Err(StatusCode::UNAUTHORIZED)
        ));

        headers.remove(crate::softprobe_assertion::ASSERTION_HEADER);
        assert!(matches!(
            lisa_service_auth_for_route(
                &Method::GET,
                "/v1/sessions/session-1/recording",
                &headers,
                "service-secret"
            ),
            Err(StatusCode::FORBIDDEN)
        ));
    }

    #[test]
    fn ordinary_bearer_credentials_are_not_treated_as_lisa_service_auth() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer regular-api-key".parse().unwrap(),
        );
        assert!(authenticate_lisa_service(&headers, "service-secret")
            .expect("ordinary auth continues")
            .is_none());
    }

    #[test]
    fn lisa_service_auth_is_limited_to_existing_session_and_score_routes() {
        for (method, path) in [
            (&Method::POST, "/v1/sessions/search"),
            (&Method::GET, "/v1/sessions/session-1"),
            (&Method::POST, "/v1/scores"),
        ] {
            assert!(is_lisa_service_route(method, path), "{method} {path}");
        }

        for (method, path) in [
            (&Method::POST, "/v1/sessions/session-1"),
            (&Method::GET, "/v1/sessions/session-1/recording"),
            (&Method::POST, "/v1/traces"),
            (&Method::POST, "/v1/score-configs"),
        ] {
            assert!(!is_lisa_service_route(method, path), "{method} {path}");
        }
    }

    fn lisa_headers(token: &str, workspace_id: &str) -> axum::http::HeaderMap {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(token.as_bytes()).unwrap();
        mac.update(format!("thelake-lisa-workspace-v1:{workspace_id}").as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers.insert("x-thelake-workspace-id", workspace_id.parse().unwrap());
        headers.insert("x-thelake-workspace-signature", signature.parse().unwrap());
        headers
    }

    #[test]
    fn explicit_local_anonymous_mode_requires_a_valid_workspace_uuid() {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap();
        let previous_mode = std::env::var("SOFTPROBE_LOCAL_ANONYMOUS").ok();
        let previous_workspace = std::env::var("THELAKE_DEFAULT_WORKSPACE_ID").ok();

        std::env::set_var("SOFTPROBE_LOCAL_ANONYMOUS", "1");
        std::env::remove_var("THELAKE_DEFAULT_WORKSPACE_ID");
        assert_eq!(
            local_anonymous_workspace_id(),
            Err(StatusCode::SERVICE_UNAVAILABLE)
        );
        std::env::set_var("THELAKE_DEFAULT_WORKSPACE_ID", "not-a-uuid");
        assert_eq!(
            local_anonymous_workspace_id(),
            Err(StatusCode::SERVICE_UNAVAILABLE)
        );
        std::env::set_var(
            "THELAKE_DEFAULT_WORKSPACE_ID",
            "550e8400-e29b-41d4-a716-446655440000",
        );
        assert_eq!(
            local_anonymous_workspace_id().unwrap().as_deref(),
            Some("550e8400-e29b-41d4-a716-446655440000")
        );

        match previous_mode {
            Some(value) => std::env::set_var("SOFTPROBE_LOCAL_ANONYMOUS", value),
            None => std::env::remove_var("SOFTPROBE_LOCAL_ANONYMOUS"),
        }
        match previous_workspace {
            Some(value) => std::env::set_var("THELAKE_DEFAULT_WORKSPACE_ID", value),
            None => std::env::remove_var("THELAKE_DEFAULT_WORKSPACE_ID"),
        }
    }
}
