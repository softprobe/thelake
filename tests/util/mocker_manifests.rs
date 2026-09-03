//! Shared helpers to locate `docs/promotion/mocker-v1.yaml` promotion contract.

use std::path::PathBuf;

/// Resolve the path to the canonical mocker-v1 telemetry_columns manifest.
///
/// `SP_MOCKER_MANIFEST` overrides the default under `docs/promotion/`.
pub fn mocker_v1_manifest_path() -> PathBuf {
    if let Ok(path) = std::env::var("SP_MOCKER_MANIFEST") {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("docs/promotion/mocker-v1.yaml")
}
