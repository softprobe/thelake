use crate::api::auth::parse_bearer;
use crate::api::AppState;
use crate::authn::WorkspaceAuth;
use crate::promotion::{
    business_current_view_name, business_physical_table_name, parse_promotion_manifest,
    BusinessApplyError, BusinessTableManifest, PromotionDataType, PromotionManifest,
    TelemetryColumnsManifest, TelemetryTable,
};
use crate::workspace::ScopeProvisioningRequest;
use crate::workspace_scope::{SharedScopeError, WorkspaceScopeMode};
use axum::{
    extract::{Extension, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

fn admin_provision_token_matches(token: &str) -> bool {
    let Ok(want) = std::env::var("SOFTPROBE_ADMIN_API_KEY") else {
        return false;
    };
    let want = want.trim();
    !want.is_empty() && want == token.trim()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceProvisionHttpRequest {
    workspace_id: String,
    #[serde(default)]
    storage_hints: Option<WorkspaceStorageHintsBody>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceStorageHintsBody {
    ducklake_metadata_schema: Option<String>,
    ducklake_data_path: Option<String>,
    gcs_bucket: Option<String>,
}

/// `POST /v1/workspaces` — admin-only tenant provisioning.
async fn v1_provision_scope(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<WorkspaceProvisionHttpRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": {"code": "unauthorized", "message": "Authorization header required"}})),
        ))?;
    let token = parse_bearer(auth).ok_or((
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": {"code": "unauthorized", "message": "Bearer token required"}})),
    ))?;
    if !admin_provision_token_matches(&token) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(
                json!({"error": {"code": "admin_required", "message": "admin API key required for tenant provisioning"}}),
            ),
        ));
    }

    let workspace_id = match crate::softprobe_assertion::parse_workspace_id(&body.workspace_id) {
        Ok(id) => id,
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": {
                        "code": "invalid_request",
                        "message": err.to_string()
                    }
                })),
            ));
        }
    };
    if crate::self_monitoring::is_reserved_workspace_id(&workspace_id) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "code": "reserved_workspace_id",
                    "message": "workspaceId is reserved for self-monitoring and cannot be provisioned via POST /v1/workspaces"
                }
            })),
        ));
    }

    let workspaces = &state.workspaces;

    let hints = body.storage_hints.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"code": "invalid_request", "message": "storageHints is required"}})),
        )
    })?;
    let metadata_schema = hints.ducklake_metadata_schema.clone().unwrap_or_default();
    let data_path = hints.ducklake_data_path.clone().unwrap_or_default();
    if metadata_schema.trim().is_empty() || data_path.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": {"code": "invalid_request", "message": "storageHints.ducklakeMetadataSchema and ducklakeDataPath are required"}}),
            ),
        ));
    }

    if let Ok(existing) = workspaces.scope_storage_hints(&workspace_id).await {
        if existing.matches_warehouse_hints(&metadata_schema, &data_path) {
            let mut scope = json!({
                "ducklakeMetadataSchema": existing.metadata_schema,
                "ducklakeDataPath": existing.data_path,
            });
            if let Some(b) = hints.gcs_bucket.as_ref().filter(|s| !s.trim().is_empty()) {
                scope["gcsBucket"] = json!(b);
            }
            return Ok(Json(json!({
                "version": 1,
                "workspaceId": workspace_id,
                "status": "exists",
                "scope": scope
            })));
        }
        return Err((
            StatusCode::CONFLICT,
            Json(
                json!({"error": {"code": "workspace_scope_conflict", "message": "workspace exists with different storage scope"}}),
            ),
        ));
    }

    let scope = workspaces
        .provision_scope(ScopeProvisioningRequest {
            scope_id: workspace_id.clone(),
            metadata_schema: metadata_schema.clone(),
            data_path: data_path.clone(),
        })
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"code": "provision_failed", "message": e.to_string()}})),
            )
        })?;

    let mut scope_json = json!({
        "ducklakeMetadataSchema": scope.metadata_schema,
        "ducklakeDataPath": scope.data_path,
    });
    if let Some(b) = hints.gcs_bucket.as_ref().filter(|s| !s.trim().is_empty()) {
        scope_json["gcsBucket"] = json!(b);
    }

    state.workspaces.invalidate(&workspace_id);

    Ok(Json(json!({
        "version": 1,
        "workspaceId": workspace_id,
        "status": "created",
        "scope": scope_json
    })))
}

async fn v1_meta() -> impl IntoResponse {
    Json(json!({
        "runtimeVersion": env!("CARGO_PKG_VERSION"),
        "specVersion": "http-control-api@v1",
        "schemaVersion": "1"
    }))
}

pub fn runtime_control_routes() -> Router<AppState> {
    Router::new()
        .route("/v1/workspaces", post(v1_provision_scope))
        .route("/v1/meta", get(v1_meta))
        .route("/v1/data/ducklake-connection", get(v1_ducklake_connection))
        .route("/v1/promotions/apply", post(v1_promotions_apply))
}

async fn v1_ducklake_connection(
    State(state): State<AppState>,
    Extension(auth): Extension<WorkspaceAuth>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    if state.workspaces.config().ducklake.workspace_scope_mode == WorkspaceScopeMode::Shared {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": {
                    "code": "shared_scope_connection_unavailable",
                    "message": "direct DuckLake connection material is unavailable in shared scope mode"
                }
            })),
        ));
    }
    match state
        .workspaces
        .ducklake_connection_material_for(&auth)
        .await
    {
        Ok(material) => Ok(Json(material)),
        Err(err) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": {
                    "code": "ducklake_connection_unavailable",
                    "message": err
                }
            })),
        )),
    }
}

#[derive(Debug, Deserialize)]
struct PromotionApplyRequest {
    #[serde(rename = "manifestYaml")]
    manifest_yaml: String,
}

async fn v1_promotions_apply(
    State(state): State<AppState>,
    Extension(tenant): Extension<WorkspaceAuth>,
    Json(req): Json<PromotionApplyRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let manifest = parse_promotion_manifest(&req.manifest_yaml).map_err(|err| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": {
                    "code": err.code(),
                    "message": err.to_string()
                }
            })),
        )
    })?;
    match manifest {
        PromotionManifest::TelemetryColumns(spec) => {
            apply_telemetry_promotion(state, tenant, req.manifest_yaml, spec).await
        }
        PromotionManifest::BusinessTable(spec) => {
            apply_business_table_promotion(state, tenant, req.manifest_yaml, spec).await
        }
    }
}

async fn apply_telemetry_promotion(
    state: AppState,
    auth: WorkspaceAuth,
    manifest_yaml: String,
    spec: TelemetryColumnsManifest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let ws = state
        .workspace_for_auth(&auth)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    let tables = telemetry_table_names(&spec.target.tables);
    ws.admin()
        .apply_and_record_telemetry_promotion(&manifest_yaml, &spec, &tables)
        .await
        .map_err(|err| {
            promotion_apply_error_preserving_shared_scope("promotion_schema_apply_failed", err)
        })?;
    Ok(Json(json!({
        "specVersion": "softprobe.promotion.apply.v1",
        "applied": true,
        "target": {
            "kind": "telemetry_columns",
            "tables": tables
        },
        "schemaChanges": telemetry_schema_changes(&spec)
    })))
}

async fn apply_business_table_promotion(
    state: AppState,
    auth: WorkspaceAuth,
    manifest_yaml: String,
    spec: BusinessTableManifest,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let ws = state
        .workspace_for_auth(&auth)
        .await
        .map_err(|err| promotion_apply_error("ducklake_scope_unavailable", err))?;
    ws.admin()
        .apply_business_promotion_guarded(&manifest_yaml, &spec)
        .await
        .map_err(|err| match err {
            BusinessApplyError::Incompatible(e) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": {
                        "code": e.code(),
                        "message": e.to_string(),
                        "path": e.path()
                    }
                })),
            ),
            BusinessApplyError::Other(e) => {
                promotion_apply_error_preserving_shared_scope("promotion_schema_apply_failed", e)
            }
        })?;
    Ok(Json(json!({
        "specVersion": "softprobe.promotion.apply.v1",
        "applied": true,
        "target": {
            "kind": "business_table",
            "table": spec.target.table,
            "version": spec.target.version
        },
        "schemaChanges": business_schema_changes(&spec)
    })))
}

fn promotion_apply_error(
    code: &'static str,
    err: anyhow::Error,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": {
                "code": code,
                "message": err.to_string()
            }
        })),
    )
}

fn promotion_apply_error_preserving_shared_scope(
    fallback_code: &'static str,
    err: anyhow::Error,
) -> (StatusCode, Json<serde_json::Value>) {
    let code = err
        .downcast_ref::<SharedScopeError>()
        .map(|error| error.code().as_str())
        .unwrap_or(fallback_code);
    promotion_apply_error(code, err)
}

fn telemetry_table_names(tables: &[TelemetryTable]) -> Vec<String> {
    tables
        .iter()
        .map(|table| match table {
            TelemetryTable::Traces => "traces",
            TelemetryTable::Logs => "logs",
        })
        .map(str::to_string)
        .collect()
}

fn telemetry_schema_changes(spec: &TelemetryColumnsManifest) -> Vec<serde_json::Value> {
    let mut changes = Vec::new();
    for table in telemetry_table_names(&spec.target.tables) {
        for col in &spec.columns {
            changes.push(json!({
                "table": table,
                "action": "add_column",
                "column": col.name,
                "type": promotion_type_name(&col.data_type),
                "nullable": col.nullable
            }));
        }
    }
    changes
}

fn business_schema_changes(spec: &BusinessTableManifest) -> Vec<serde_json::Value> {
    let table = business_physical_table_name(spec);
    let view = business_current_view_name(spec);
    vec![
        json!({
            "action": "create_table",
            "table": table
        }),
        json!({
            "action": "create_or_replace_view",
            "view": view,
            "sourceTable": table
        }),
    ]
}

fn promotion_type_name(data_type: &PromotionDataType) -> &'static str {
    match data_type {
        PromotionDataType::String => "string",
        PromotionDataType::Bool => "bool",
        PromotionDataType::Int64 => "int64",
        PromotionDataType::Double => "double",
        PromotionDataType::Decimal => "decimal",
        PromotionDataType::Timestamp => "timestamp",
        PromotionDataType::Json => "json",
    }
}

#[cfg(test)]
mod tests {
    use crate::authn::WorkspaceAuth;
    use crate::config::Config;
    use crate::storage::ducklake::PhysicalScope;
    use crate::workspace::DuckLakeConnectionMaterial;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn ducklake_connection_material_uses_tenant_scope_not_config_overrides() {
        let _guard = env_lock();
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");

        let mut config = Config::default();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        config.ducklake.data_path = "./warehouse/ducklake/data/".to_string();
        config.ducklake.metadata_schema = "tenant_meta".to_string();

        let tenant = WorkspaceAuth {
            workspace_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
            agent_id: None,
            agent_name: None,
        };
        let scope = PhysicalScope::new(
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string(),
            "./warehouse/ducklake/data/".to_string(),
            "softprobe".to_string(),
            "tenant_tenant_123".to_string(),
        );

        let material = DuckLakeConnectionMaterial::from_workspace_scope(&tenant, &scope, &config)
            .expect("connection material");
        assert_eq!(material.version, 1);
        assert_eq!(material.workspace_id, "tenant-123");
        assert_eq!(
            material.ducklake_pg_uri,
            "host=pg port=5432 dbname=ducklake user=reader password=secret"
        );
        assert_eq!(material.ducklake_metadata_schema, "tenant_tenant_123");
        assert_eq!(material.ducklake_data_path, "./warehouse/ducklake/data/");
        assert_eq!(material.gcs_bucket, "softprobe-tenant-bucket");
        assert_eq!(material.gcs_hmac_access_key_id, "");
        assert_eq!(material.gcs_hmac_secret, "");
        assert_eq!(material.session_token, "");
        assert_eq!(material.schema_version, "1");
    }

    #[test]
    fn ducklake_connection_material_reads_hmac_from_environment() {
        let _guard = env_lock();
        std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", "access-id");
        std::env::set_var("GCS_HMAC_SECRET", "secret-value");

        let mut config = Config::default();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();

        let tenant = WorkspaceAuth {
            workspace_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
            agent_id: None,
            agent_name: None,
        };
        let scope = PhysicalScope::new(
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string(),
            "gs://bucket/ducklake/data/".to_string(),
            "softprobe".to_string(),
            "tenant_tenant_123".to_string(),
        );

        let material = DuckLakeConnectionMaterial::from_workspace_scope(&tenant, &scope, &config)
            .expect("connection material");
        assert_eq!(material.ducklake_metadata_schema, "tenant_tenant_123");
        assert_eq!(material.ducklake_data_path, "gs://bucket/ducklake/data/");
        assert_eq!(material.gcs_hmac_access_key_id, "access-id");
        assert_eq!(material.gcs_hmac_secret, "secret-value");
        assert_eq!(material.session_token, "");

        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");
    }

    #[test]
    fn ducklake_connection_material_includes_s3_session_token() {
        let _guard = env_lock();
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIATEST");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret-test");
        std::env::set_var("AWS_SESSION_TOKEN", "session-test-token");
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");

        let mut config = Config::default();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();
        config.object_store.region = "us-west-2".to_string();

        let tenant = WorkspaceAuth {
            workspace_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
            agent_id: None,
            agent_name: None,
        };
        let scope = PhysicalScope::new(
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string(),
            "s3://bucket/ducklake/data/".to_string(),
            "softprobe".to_string(),
            "tenant_tenant_123".to_string(),
        );

        let material = DuckLakeConnectionMaterial::from_workspace_scope(&tenant, &scope, &config)
            .expect("connection material");
        assert_eq!(material.gcs_hmac_access_key_id, "AKIATEST");
        assert_eq!(material.gcs_hmac_secret, "secret-test");
        assert_eq!(material.session_token, "session-test-token");

        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
    }

    #[test]
    fn ducklake_connection_material_requires_hmac_for_gcs_path() {
        let _guard = env_lock();
        std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCS_HMAC_SECRET");
        std::env::remove_var("GCP_HMAC_ACCESS_KEY_ID");
        std::env::remove_var("GCP_HMAC_SECRET");
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");

        let mut config = Config::default();
        config.ducklake.metadata_path =
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string();

        let tenant = WorkspaceAuth {
            workspace_id: "tenant-123".to_string(),
            bucket_name: "softprobe-tenant-bucket".to_string(),
            dataset_id: "ignored".to_string(),
            agent_id: None,
            agent_name: None,
        };
        let scope = PhysicalScope::new(
            "host=pg port=5432 dbname=ducklake user=reader password=secret".to_string(),
            "gs://bucket/ducklake/data/".to_string(),
            "softprobe".to_string(),
            "tenant_tenant_123".to_string(),
        );

        let err = DuckLakeConnectionMaterial::from_workspace_scope(&tenant, &scope, &config)
            .expect_err("missing hmac should fail");
        assert!(err.contains("GCS_HMAC_ACCESS_KEY_ID") || err.contains("object-store credentials"));
    }

    #[test]
    fn scope_storage_hints_match_warehouse_identity() {
        use crate::workspace::ScopeStorageHints;
        let hints = ScopeStorageHints {
            metadata_schema: "meta_a".into(),
            data_path: "/data/a".into(),
        };
        assert!(hints.matches_warehouse_hints("meta_a", "/data/a"));
        assert!(!hints.matches_warehouse_hints("meta_b", "/data/a"));
        assert!(!hints.matches_warehouse_hints("meta_a", "/data/b"));
    }
}
