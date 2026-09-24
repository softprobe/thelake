//! Build opaque [`PhysicalScope`] from POJO DuckLakeConfig fields (test boundary).

use softprobe_runtime::config::DuckLakeConfig;
use softprobe_runtime::workspace_scope::PhysicalScope;

/// Construct a [`PhysicalScope`] via config POJO fields (no public PhysicalScope::new).
pub fn physical_scope(
    metadata_path: impl Into<String>,
    data_path: impl Into<String>,
    metadata_schema: impl Into<String>,
) -> PhysicalScope {
    let mut ducklake = DuckLakeConfig::default();
    ducklake.metadata_path = metadata_path.into();
    ducklake.data_path = data_path.into();
    ducklake.metadata_schema = metadata_schema.into();
    ducklake.catalog_alias = "softprobe".to_string();
    PhysicalScope::from_ducklake(&ducklake)
}
