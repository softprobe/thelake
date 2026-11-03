use crate::api::AppState;
use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware::Next,
    response::Response,
};

/// Prefer `X-Softprobe-Assertion` (sp-llm#39). Fall back to Bearer assertion JWT,
/// then optional `SOFTPROBE_DEFAULT_WORKSPACE_ID`, then Softprobe auth service.
pub async fn runtime_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    if req.method() == Method::OPTIONS {
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
        return run_tenant_request(state, req, next, info).await;
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
                    return run_tenant_request(state, req, next, info).await;
                }
            }
        }
    }

    // Legacy direct-to-thelake clients: no assertion header. Optional default
    // lake routes them onto a configured MAP-ready scope (Bearer still required).
    if let Some(default_key) = crate::softprobe_assertion::default_workspace_id_from_env() {
        let info = crate::softprobe_assertion::workspace_auth_for_default_lake(&default_key)
            .map_err(|_| StatusCode::FORBIDDEN)?;
        return run_tenant_request(state, req, next, info).await;
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

    run_tenant_request(state, req, next, info).await
}

async fn run_tenant_request(
    state: AppState,
    mut req: Request,
    next: Next,
    info: crate::authn::WorkspaceAuth,
) -> Result<Response, StatusCode> {
    let workspace_id = info.workspace_id.clone();
    req.extensions_mut().insert(info);
    state
        .workspaces
        .with_active_workspace_access(&workspace_id, next.run(req))
        .await
        .map_err(|error| {
            tracing::warn!(workspace_id, "workspace binding is not active: {error}");
            StatusCode::FORBIDDEN
        })
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
    if *method == Method::DELETE && is_workspace_deletion_route(path) {
        return false;
    }
    if *method == Method::GET && is_workspace_deletion_status_route(path) {
        return false;
    }
    is_authenticated_api_prefix(path)
}

fn is_workspace_deletion_route(path: &str) -> bool {
    path.strip_prefix("/v1/workspaces/")
        .is_some_and(|workspace_id| !workspace_id.is_empty() && !workspace_id.contains('/'))
}

fn is_workspace_deletion_status_route(path: &str) -> bool {
    path.strip_prefix("/v1/workspaces/")
        .and_then(|suffix| suffix.strip_suffix("/deletion"))
        .is_some_and(|workspace_id| !workspace_id.is_empty() && !workspace_id.contains('/'))
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
        assert!(
            !requires_runtime_auth(
                &Method::DELETE,
                "/v1/workspaces/550e8400-e29b-41d4-a716-446655440000"
            ),
            "workspace deletion uses admin Bearer validated in-handler"
        );
        assert!(
            !requires_runtime_auth(
                &Method::GET,
                "/v1/workspaces/550e8400-e29b-41d4-a716-446655440000/deletion"
            ),
            "workspace deletion status uses admin Bearer validated in-handler"
        );
        assert!(requires_runtime_auth(
            &Method::GET,
            "/v1/workspaces/550e8400-e29b-41d4-a716-446655440000"
        ));
        assert!(requires_runtime_auth(
            &Method::DELETE,
            "/v1/workspaces/550e8400-e29b-41d4-a716-446655440000/nested"
        ));
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
}
