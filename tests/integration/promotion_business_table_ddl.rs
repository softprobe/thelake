use softprobe_runtime::config::Config;
use softprobe_runtime::promotion::{
    business_table_create_ddls, parse_promotion_manifest, PromotionManifest,
};
use softprobe_runtime::runtime_engine::{RuntimeEngineManager, ScopeProvisioningRequest};
use std::sync::Arc;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::util::config::apply_workspace_scope_mode;

#[test]
fn business_table_ddl_uses_timestamp_ns_for_time_fields() {
    let manifest = parse_promotion_manifest(
        r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
  - name: placed_at
    type: timestamp
    nullable: true
    source:
      from: attribute
      key: order.placed_at
"#,
    )
    .expect("valid manifest");
    let PromotionManifest::BusinessTable(spec) = manifest else {
        panic!("expected business table manifest");
    };

    let ddls = business_table_create_ddls(r#""softprobe"."tenant_business""#, &spec)
        .expect("DuckLake business table DDL");
    let create_table = &ddls[0];
    assert!(create_table.contains(r#""event_timestamp" TIMESTAMP_NS"#));
    assert!(create_table.contains(r#""source_timestamp" TIMESTAMP_NS"#));
    assert!(create_table.contains(r#""placed_at" TIMESTAMP_NS"#));
    assert!(ddls.last().unwrap().contains("checkout_orders_current"));
}

#[tokio::test]
async fn ducklake_writer_applies_business_table_to_tenant_scope() {
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().to_string().replace('-', "_");
    let mut config = Config::default();
    config.ducklake.metadata_path =
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake".to_string();
    config.ducklake.catalog_alias = "softprobe".to_string();
    // Keep schema names ≤63 chars (Postgres identifier limit) to avoid silent truncation collisions.
    let short = &suffix[..8.min(suffix.len())];
    // Shared mode binds to the process-default catalog; keep request schema identical.
    let business_metadata_schema = format!("sp_biz_{short}");
    config.ducklake.metadata_schema = business_metadata_schema.clone();
    let business_data_path = temp.path().join("data").to_string_lossy().to_string();
    config.ducklake.data_path = business_data_path.clone();
    config.ducklake.data_inlining_row_limit = Some(0);
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().to_string());
    apply_workspace_scope_mode(&mut config);

    let manager = RuntimeEngineManager::connect(Arc::new(config.clone()), None)
        .await
        .expect("connect runtime engines");
    let business_workspace_id = Uuid::new_v4().to_string();
    let _hints = manager
        .provision_scope(ScopeProvisioningRequest {
            scope_id: business_workspace_id.clone(),
            metadata_schema: business_metadata_schema.clone(),
            data_path: business_data_path,
        })
        .await
        .expect("provision workspace");
    let engine = manager
        .engine_for(&business_workspace_id)
        .await
        .expect("workspace engine");
    let manifest = parse_promotion_manifest(BUSINESS_MANIFEST).expect("valid manifest");
    let PromotionManifest::BusinessTable(spec) = manifest else {
        panic!("expected business table manifest");
    };

    let spec_id = match engine
        .apply_business_promotion(BUSINESS_MANIFEST, &spec)
        .await
    {
        Ok(spec_id) => spec_id,
        Err(_) => panic!("apply business table promotion"),
    };
    assert!(!spec_id.is_empty());
    assert_ducklake_table_exists(&business_metadata_schema, "checkout_orders_v1").await;
    assert_ducklake_view_exists(&business_metadata_schema, "checkout_orders_current").await;
}

async fn assert_ducklake_table_exists(schema: &str, table: &str) {
    assert_ducklake_relation_exists(schema, "ducklake_table", "table_name", table).await;
}

async fn assert_ducklake_view_exists(schema: &str, view: &str) {
    assert_ducklake_relation_exists(schema, "ducklake_view", "view_name", view).await;
}

async fn assert_ducklake_relation_exists(
    schema: &str,
    metadata_table: &str,
    name_column: &str,
    relation: &str,
) {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
        NoTls,
    )
    .await
    .expect("connect ducklake postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let sql = format!(
        r#"SELECT count(*) FROM "{}".{} WHERE {} = $1;"#,
        schema, metadata_table, name_column
    );
    let count: i64 = client
        .query_one(&sql, &[&relation])
        .await
        .expect("query DuckLake relation metadata")
        .get(0);
    assert_eq!(count, 1, "missing DuckLake relation {schema}.{relation}");
}

const BUSINESS_MANIFEST: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;
