use softprobe_runtime::config::Config;
use softprobe_runtime::promotion::{parse_promotion_manifest, PromotionManifest};
use softprobe_runtime::runtime_engine::{RuntimeEngineManager, ScopeProvisioningRequest};
use std::sync::Arc;
use uuid::Uuid;

// Use logs (not traces): recording a traces promo would supersede the product
// hot-attrs seeded on provision, which must stay complete or ensure panics.
const MANIFEST_DIVISION: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [logs]
columns:
  - name: division_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: division.name
"#;

const MANIFEST_REGION: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [logs]
columns:
  - name: region_code
    type: string
    nullable: true
    source:
      from: attribute
      key: region.code
"#;

#[tokio::test]
async fn resolve_scope_is_registry_strict_and_idempotent() {
    let manager = postgres_manager().await;
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let tenant_id = format!("tenant_registry_{suffix}");
    let metadata_schema = format!("tenant_registry_scope_{suffix}");
    let data_path = format!("./target/registry-test-data/{tenant_id}/");

    let unknown = manager
        .resolve_scope(&tenant_id)
        .await
        .expect_err("unknown scopes must not be lazily provisioned");
    assert!(
        unknown.to_string().contains("unknown scope"),
        "unexpected unknown scope error: {unknown}"
    );

    let request = ScopeProvisioningRequest {
        scope_id: tenant_id.clone(),
        metadata_schema: metadata_schema.clone(),
        data_path: data_path.clone(),
    };
    let created = manager
        .provision_scope(request.clone())
        .await
        .expect("provision tenant");
    let first = manager
        .resolve_scope(&tenant_id)
        .await
        .expect("first resolve");
    let second = manager
        .resolve_scope(&tenant_id)
        .await
        .expect("second resolve");

    assert_eq!(created, first);
    assert_eq!(first, second);

    let repeated = manager
        .provision_scope(request)
        .await
        .expect("idempotent provision");
    assert_eq!(repeated, first);
}

#[tokio::test]
async fn resolver_loads_active_promotion_specs_from_only_the_resolved_tenant_schema() {
    let manager = postgres_manager().await;
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let tenant_a = format!("tenant_promo_registry_a_{suffix}");
    let tenant_b = format!("tenant_promo_registry_b_{suffix}");

    let scope_a = manager
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_a.clone(),
            // PostgreSQL truncates identifiers at 63 bytes. Keep the
            // generated schema name comfortably below that limit so the
            // registry contract does not accidentally create a truncated
            // catalog that cannot be re-attached by DuckLake.
            metadata_schema: format!("promo_a_{suffix}"),
            data_path: format!("./target/registry-test-data/{tenant_a}/"),
        })
        .await
        .expect("provision tenant A");
    let scope_b = manager
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_b.clone(),
            metadata_schema: format!("promo_b_{suffix}"),
            data_path: format!("./target/registry-test-data/{tenant_b}/"),
        })
        .await
        .expect("provision tenant B");

    let engine_a = manager
        .engine_for(&tenant_a)
        .await
        .expect("tenant A engine");
    let engine_b = manager
        .engine_for(&tenant_b)
        .await
        .expect("tenant B engine");
    let PromotionManifest::TelemetryColumns(spec_a) =
        parse_promotion_manifest(MANIFEST_DIVISION).expect("tenant A manifest")
    else {
        panic!("expected telemetry manifest for tenant A");
    };
    let PromotionManifest::TelemetryColumns(spec_b) =
        parse_promotion_manifest(MANIFEST_REGION).expect("tenant B manifest")
    else {
        panic!("expected telemetry manifest for tenant B");
    };
    engine_a
        .admin
        .apply_and_record_telemetry_promotion(MANIFEST_DIVISION, &spec_a, &["logs".to_string()])
        .await
        .expect("record tenant A spec");
    engine_b
        .admin
        .apply_and_record_telemetry_promotion(MANIFEST_REGION, &spec_b, &["logs".to_string()])
        .await
        .expect("record tenant B spec");

    let (resolved_a, manifests_a) = manager
        .load_active_telemetry_columns_manifests(&tenant_a)
        .await
        .expect("load tenant A manifests");
    let (resolved_b, manifests_b) = manager
        .load_active_telemetry_columns_manifests(&tenant_b)
        .await
        .expect("load tenant B manifests");

    assert_eq!(resolved_a, scope_a);
    assert_eq!(resolved_b, scope_b);

    let names_a: Vec<&str> = manifests_a
        .iter()
        .flat_map(|m| m.columns.iter().map(|c| c.name.as_str()))
        .collect();
    let names_b: Vec<&str> = manifests_b
        .iter()
        .flat_map(|m| m.columns.iter().map(|c| c.name.as_str()))
        .collect();
    assert!(
        names_a.contains(&"division_name"),
        "tenant A must see its logs promo: {names_a:?}"
    );
    assert!(
        names_b.contains(&"region_code"),
        "tenant B must see its logs promo: {names_b:?}"
    );
    assert!(
        !names_a.contains(&"region_code"),
        "tenant A must not see tenant B's column"
    );
    assert!(
        !names_b.contains(&"division_name"),
        "tenant B must not see tenant A's column"
    );
}

async fn postgres_manager() -> RuntimeEngineManager {
    RuntimeEngineManager::connect(Arc::new(postgres_config()), None)
        .await
        .expect("connect runtime engine manager")
}

fn postgres_config() -> Config {
    let mut config = Config::default();
    config.ducklake.writer_pool_size = 1;
    config.query.max_connections = 1;
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.metadata_schema = "softprobe_registry_test".to_string();
    // This registry contract exercises Postgres metadata only. Keep the
    // DuckLake data path local so the test never probes cloud instance
    // metadata for credentials while building a tenant-bound engine.
    config.ducklake.data_path = "./warehouse/ducklake/registry-test-data/".to_string();

    config
}
