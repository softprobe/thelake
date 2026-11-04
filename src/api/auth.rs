use crate::api::AppState;
use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware::Next,
    response::Response,
};

fn authorize_agent_assertion_request(
    claims: &crate::softprobe_assertion::SoftprobeAssertionClaims,
    method: &Method,
    path: &str,
) -> Result<(), StatusCode> {
    if claims.capture_id.is_some() && (claims.agent_id.is_none() || claims.capabilities.is_none()) {
        return Err(StatusCode::FORBIDDEN);
    }
    if claims.capture_id.is_some() && (*method != Method::POST || path != "/v1/traces") {
        return Err(StatusCode::FORBIDDEN);
    }
    let has_agent_owner = claims
        .agent_id
        .as_deref()
        .is_some_and(|agent_id| !agent_id.trim().is_empty());

    let has_agent_claim =
        claims.agent_id.is_some() || claims.agent_name.is_some() || claims.capabilities.is_some();
    if !has_agent_claim {
        return Ok(());
    }
    if !has_agent_owner {
        return Err(StatusCode::FORBIDDEN);
    }

    let has_ingest_capability = claims
        .capabilities
        .as_ref()
        .is_some_and(|capabilities| capabilities.iter().any(|capability| capability == "ingest"));
    let is_ingest_route =
        *method == Method::POST && matches!(path, "/v1/traces" | "/v1/logs" | "/v1/scores");

    if has_ingest_capability && is_ingest_route {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

/// Prefer `X-Softprobe-Assertion` (sp-llm#39). Fall back to Bearer assertion JWT,
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
        authorize_agent_assertion_request(&claims, req.method(), path)?;
        if let Some(capture_id) = claims.capture_id.as_deref() {
            let capture_id = uuid::Uuid::parse_str(capture_id)
                .map_err(|_| StatusCode::FORBIDDEN)?
                .to_string();
            req.extensions_mut()
                .insert(crate::softprobe_assertion::CaptureProfileContext { capture_id });
        }
        let info = crate::softprobe_assertion::workspace_auth_from_assertion(&claims)
            .map_err(|_| StatusCode::FORBIDDEN)?;
        return Ok(run_workspace_request(req, next, info).await);
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
                authorize_agent_assertion_request(&claims, req.method(), path)?;
                if let Some(capture_id) = claims.capture_id.as_deref() {
                    let capture_id = uuid::Uuid::parse_str(capture_id)
                        .map_err(|_| StatusCode::FORBIDDEN)?
                        .to_string();
                    req.extensions_mut()
                        .insert(crate::softprobe_assertion::CaptureProfileContext { capture_id });
                }
                let info = crate::softprobe_assertion::workspace_auth_from_assertion(&claims)
                    .map_err(|_| StatusCode::FORBIDDEN)?;
                return Ok(run_workspace_request(req, next, info).await);
            }
        }
    }

    // Explorer's opaque API keys are resolved by the gateway and are never
    // valid direct-to-TheLake bearer credentials. Do not let them reach the
    // legacy default-workspace fallback, which cannot verify their owner.
    if token.starts_with("spk_") {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Legacy direct-to-thelake clients: no assertion header. Optional default
    // lake routes them onto a configured MAP-ready scope (Bearer still required).
    if let Some(default_key) = crate::softprobe_assertion::default_workspace_id_from_env() {
        let info = crate::softprobe_assertion::workspace_auth_for_default_lake(&default_key)
            .map_err(|_| StatusCode::FORBIDDEN)?;
        return Ok(run_workspace_request(req, next, info).await);
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

    Ok(run_workspace_request(req, next, info).await)
}

async fn run_workspace_request(
    mut req: Request,
    next: Next,
    info: crate::authn::WorkspaceAuth,
) -> Response {
    req.extensions_mut().insert(info);
    next.run(req).await
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

/// Paths that require Bearer → workspace resolution (OTLP/control + compatibility stubs).
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
    use axum::{
        body::Body, middleware::from_fn_with_state, response::IntoResponse, routing::post, Router,
    };
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use tower::ServiceExt;

    const TEST_SECRET: &str = "auth-middleware-test-secret";
    const TEST_WORKSPACE: &str = "550e8400-e29b-41d4-a716-446655440000";

    struct EnvSnapshot(Vec<(&'static str, Option<String>)>);

    impl EnvSnapshot {
        fn capture(keys: &[&'static str]) -> Self {
            Self(
                keys.iter()
                    .map(|key| (*key, std::env::var(key).ok()))
                    .collect(),
            )
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            for (key, value) in self.0.drain(..) {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    async fn probe() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    async fn capture_scope_probe(
        capture: Option<axum::Extension<crate::softprobe_assertion::CaptureProfileContext>>,
    ) -> axum::response::Response {
        let mut response = StatusCode::NO_CONTENT.into_response();
        if let Some(axum::Extension(capture)) = capture {
            response.headers_mut().insert(
                "x-test-capture-id",
                capture.capture_id.parse().expect("valid header value"),
            );
        }
        response
    }

    fn middleware_test_router() -> Router {
        let state = crate::api::AppState {
            workspaces: std::sync::Arc::new(
                crate::workspace::WorkspaceManager::for_auth_middleware_test(),
            ),
        };
        Router::new()
            .route("/v1/traces", post(capture_scope_probe).options(probe))
            .route("/v1/logs", post(probe))
            .route("/v1/scores", post(probe))
            .route("/v1/sessions/search", post(probe))
            .merge(crate::api::control::runtime_control_routes())
            .merge(crate::compat::loki::loki_routes())
            .merge(crate::compat::tempo::tempo_routes())
            .with_state(state.clone())
            .layer(from_fn_with_state(state, runtime_auth_middleware))
    }

    fn signed_assertion(
        secret: &str,
        workspace_id: &str,
        agent_id: Option<&str>,
        capabilities: Option<&[&str]>,
        capture_id: Option<&str>,
    ) -> String {
        let mut payload = serde_json::json!({
            "iss": "softprobe-edge",
            "aud": "sp-backend",
            "sub": "auth-test-subject",
            "workspace_id": workspace_id,
            "exp": chrono::Utc::now().timestamp() + 300
        });
        if let Some(agent_id) = agent_id {
            payload["agent_id"] = serde_json::json!(agent_id);
        }
        if let Some(capabilities) = capabilities {
            payload["capabilities"] = serde_json::json!(capabilities);
        }
        if let Some(capture_id) = capture_id {
            payload["capture_id"] = serde_json::json!(capture_id);
        }
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(payload.to_string().as_bytes());
        let signing_input = format!("{header}.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{signing_input}.{signature}")
    }

    async fn response_status(
        app: Router,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<&str>,
    ) -> StatusCode {
        let mut request = axum::http::Request::builder().method(method).uri(path);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let body = body
            .map(|body| Body::from(body.to_string()))
            .unwrap_or_else(Body::empty);
        app.oneshot(request.body(body).unwrap())
            .await
            .unwrap()
            .status()
    }

    fn configure_auth_environment() -> EnvSnapshot {
        let snapshot = EnvSnapshot::capture(&[
            "SOFTPROBE_ASSERTION_HMAC_SECRET",
            "ASSERTION_HMAC_SECRET",
            "SOFTPROBE_DEFAULT_WORKSPACE_ID",
            "THELAKE_DEFAULT_WORKSPACE_ID",
            "SOFTPROBE_ADMIN_API_KEY",
        ]);
        std::env::remove_var("SOFTPROBE_ASSERTION_HMAC_SECRET");
        std::env::set_var("ASSERTION_HMAC_SECRET", TEST_SECRET);
        std::env::set_var("SOFTPROBE_DEFAULT_WORKSPACE_ID", TEST_WORKSPACE);
        std::env::remove_var("THELAKE_DEFAULT_WORKSPACE_ID");
        std::env::set_var("SOFTPROBE_ADMIN_API_KEY", "test-admin-key");
        snapshot
    }

    fn agent_assertion(
        agent_id: Option<&str>,
        capabilities: Option<Vec<&str>>,
    ) -> crate::softprobe_assertion::SoftprobeAssertionClaims {
        crate::softprobe_assertion::SoftprobeAssertionClaims {
            iss: crate::softprobe_assertion::DEFAULT_ISS.to_string(),
            aud: crate::softprobe_assertion::Aud::One(
                crate::softprobe_assertion::DEFAULT_AUD.to_string(),
            ),
            sub: "agent-key".to_string(),
            workspace_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            roles: Some(vec!["member".to_string()]),
            email: None,
            exp: None,
            agent_id: agent_id.map(str::to_string),
            agent_name: agent_id.map(|_| "Capture Agent".to_string()),
            capabilities: capabilities.map(|items| items.into_iter().map(str::to_string).collect()),
            capture_id: None,
        }
    }

    #[test]
    fn agent_assertions_require_ingest_capability_on_exact_write_routes() {
        let missing = agent_assertion(Some("agent-1"), None);
        assert_eq!(
            authorize_agent_assertion_request(&missing, &Method::POST, "/v1/traces"),
            Err(StatusCode::FORBIDDEN)
        );

        let wrong_capability = agent_assertion(Some("agent-1"), Some(vec!["read"]));
        assert_eq!(
            authorize_agent_assertion_request(&wrong_capability, &Method::POST, "/v1/traces"),
            Err(StatusCode::FORBIDDEN)
        );

        let ingest = agent_assertion(Some("agent-1"), Some(vec!["ingest"]));
        for path in ["/v1/traces", "/v1/logs", "/v1/scores"] {
            assert_eq!(
                authorize_agent_assertion_request(&ingest, &Method::POST, path),
                Ok(()),
                "Agent ingest capability should allow POST {path}"
            );
        }
        for (method, path) in [
            (Method::GET, "/v1/traces"),
            (Method::POST, "/v1/sessions/search"),
            (Method::POST, "/v1/score-configs"),
            (Method::POST, "/v1/promotions/apply"),
        ] {
            assert_eq!(
                authorize_agent_assertion_request(&ingest, &method, path),
                Err(StatusCode::FORBIDDEN),
                "Agent ingest capability must reject {method} {path}"
            );
        }
    }

    #[test]
    fn partial_agent_identity_claims_do_not_keep_workspace_user_access() {
        let mut agent_name_only = agent_assertion(None, None);
        agent_name_only.agent_name = Some("Capture Agent".to_string());
        assert_eq!(
            authorize_agent_assertion_request(&agent_name_only, &Method::POST, "/v1/traces"),
            Err(StatusCode::FORBIDDEN)
        );

        let capability_only = agent_assertion(None, Some(vec!["ingest"]));
        assert_eq!(
            authorize_agent_assertion_request(&capability_only, &Method::POST, "/v1/traces"),
            Err(StatusCode::FORBIDDEN)
        );
    }

    #[test]
    fn agent_assertions_cannot_reach_compatibility_query_routes() {
        let ingest = agent_assertion(Some("agent-1"), Some(vec!["ingest"]));
        for (method, path) in [
            (Method::GET, "/loki/api/v1/query"),
            (Method::GET, "/loki/api/v1/index/stats"),
            (Method::GET, "/api/traces/abc123"),
            (Method::GET, "/api/v2/traces/abc123"),
            (Method::GET, "/api/search"),
        ] {
            assert_eq!(
                authorize_agent_assertion_request(&ingest, &method, path),
                Err(StatusCode::FORBIDDEN),
                "Agent ingest capability must reject compatibility route {method} {path}"
            );
        }
    }

    #[test]
    fn workspace_user_assertions_keep_existing_route_access() {
        let user = agent_assertion(None, None);
        assert_eq!(
            authorize_agent_assertion_request(&user, &Method::POST, "/v1/sessions/search"),
            Ok(())
        );
    }

    #[tokio::test]
    async fn middleware_enforces_agent_capabilities_for_both_assertion_transports() {
        let _env_lock = crate::softprobe_assertion::AUTH_ENV_TEST_LOCK.lock().await;
        let _env = configure_auth_environment();
        let app = middleware_test_router();
        let agent = signed_assertion(
            TEST_SECRET,
            TEST_WORKSPACE,
            Some("agent-1"),
            Some(&["ingest"]),
            None,
        );

        for path in ["/v1/traces", "/v1/logs", "/v1/scores"] {
            assert_eq!(
                response_status(
                    app.clone(),
                    Method::POST,
                    path,
                    &[(crate::softprobe_assertion::ASSERTION_HEADER, &agent)],
                    None,
                )
                .await,
                StatusCode::NO_CONTENT,
                "assertion header should allow POST {path}"
            );
        }
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/traces",
                &[("authorization", &format!("Bearer {agent}"))],
                None,
            )
            .await,
            StatusCode::NO_CONTENT,
            "Bearer assertion should allow ingest"
        );
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/sessions/search",
                &[(crate::softprobe_assertion::ASSERTION_HEADER, &agent)],
                None,
            )
            .await,
            StatusCode::FORBIDDEN,
            "Agent assertions must be rejected before read-handler dispatch"
        );
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/sessions/search",
                &[("authorization", &format!("Bearer {agent}"))],
                None,
            )
            .await,
            StatusCode::FORBIDDEN,
            "Bearer Agent assertions must be rejected before read-handler dispatch"
        );
        for path in [
            "/loki/api/v1/query",
            "/api/traces/000102030405060708090a0b0c0d0e0f",
            "/api/search",
        ] {
            assert_eq!(
                response_status(
                    app.clone(),
                    Method::GET,
                    path,
                    &[("authorization", &format!("Bearer {agent}"))],
                    None,
                )
                .await,
                StatusCode::FORBIDDEN,
                "Agent assertion must not reach compatibility route {path}"
            );
        }

        let user = signed_assertion(TEST_SECRET, TEST_WORKSPACE, None, None, None);
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/sessions/search",
                &[("authorization", &format!("Bearer {user}"))],
                None,
            )
            .await,
            StatusCode::NO_CONTENT,
            "ordinary workspace-user reads must remain available"
        );

        let ownerless_capability =
            signed_assertion(TEST_SECRET, TEST_WORKSPACE, None, Some(&["ingest"]), None);
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/traces",
                &[(
                    crate::softprobe_assertion::ASSERTION_HEADER,
                    &ownerless_capability
                )],
                None,
            )
            .await,
            StatusCode::FORBIDDEN,
            "capability-bearing assertion without Agent owner must fail closed"
        );

        let bad_workspace_agent = signed_assertion(
            TEST_SECRET,
            "workspace-is-not-a-uuid",
            Some("agent-1"),
            Some(&["ingest"]),
            None,
        );
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/traces",
                &[("authorization", &format!("Bearer {bad_workspace_agent}"))],
                None,
            )
            .await,
            StatusCode::FORBIDDEN,
            "invalid verified Agent workspace must not fall through to default scope"
        );

        let bad_workspace_user =
            signed_assertion(TEST_SECRET, "workspace-is-not-a-uuid", None, None, None);
        assert_eq!(
            response_status(
                app.clone(),
                Method::GET,
                "/v1/meta",
                &[("authorization", &format!("Bearer {bad_workspace_user}"))],
                None,
            )
            .await,
            StatusCode::FORBIDDEN,
            "invalid verified workspace-user assertion must not fall through to default scope"
        );

        let opaque_key = format!("spk_{}", "a".repeat(24));
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/sessions/search",
                &[("authorization", &format!("Bearer {opaque_key}"))],
                None,
            )
            .await,
            StatusCode::UNAUTHORIZED,
            "opaque Explorer API keys must not use default-workspace bypass"
        );
        assert_eq!(
            response_status(
                app.clone(),
                Method::GET,
                "/v1/meta",
                &[("authorization", "Bearer legacy-local-token")],
                None,
            )
            .await,
            StatusCode::OK,
            "legacy direct-to-TheLake Bearer fallback must remain available"
        );

        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/workspaces",
                &[
                    ("authorization", &format!("Bearer {agent}")),
                    ("content-type", "application/json"),
                ],
                Some(
                    r#"{"workspaceId":"550e8400-e29b-41d4-a716-446655440001","storageHints":{"ducklakeMetadataSchema":"schema","ducklakeDataPath":"/tmp/data"}}"#,
                ),
            )
            .await,
            StatusCode::FORBIDDEN,
            "Agent assertion must not authorize admin workspace provisioning"
        );
        assert_eq!(
            response_status(
                app.clone(),
                Method::POST,
                "/v1/workspaces",
                &[
                    ("authorization", "Bearer test-admin-key"),
                    ("content-type", "application/json"),
                ],
                Some(r#"{"workspaceId":"not-a-uuid"}"#),
            )
            .await,
            StatusCode::BAD_REQUEST,
            "admin API key should still reach the workspace validation handler"
        );
        assert_eq!(
            response_status(app, Method::OPTIONS, "/v1/traces", &[], None,).await,
            StatusCode::NO_CONTENT,
            "CORS preflight should remain unauthenticated"
        );
    }

    #[tokio::test]
    async fn middleware_carries_verified_capture_profile_scope_to_ingest_handler() {
        let _env_lock = crate::softprobe_assertion::AUTH_ENV_TEST_LOCK.lock().await;
        let _env = configure_auth_environment();
        let capture_id = "550e8400-e29b-41d4-a716-446655440099";
        let assertion = signed_assertion(
            TEST_SECRET,
            TEST_WORKSPACE,
            Some("agent-1"),
            Some(&["ingest"]),
            Some(capture_id),
        );
        let response = middleware_test_router()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/v1/traces")
                    .header(crate::softprobe_assertion::ASSERTION_HEADER, assertion)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()["x-test-capture-id"], capture_id);
    }

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
