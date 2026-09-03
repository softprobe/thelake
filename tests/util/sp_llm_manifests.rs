//! Shared helpers to locate sibling `sp-llm/manifests/*.yaml` promotion contracts.
//!
//! Softprobe product profiles (llm-v1, mocker-v1) are SSOT in softprobe/sp-llm — not
//! duplicated under thelake. Tests load them from the sibling checkout (or env override).

use std::path::PathBuf;

/// Resolve the path to one `sp-llm/manifests/<name>` file.
///
/// `SP_LLM_MANIFEST_DIR` overrides the sibling-checkout default.
pub fn sp_llm_manifest_path(name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var("SP_LLM_MANIFEST_DIR") {
        return PathBuf::from(dir).join(name);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../sp-llm/manifests")
        .join(name)
}

/// Resolve the canonical mocker-v1 telemetry_columns manifest (Softprobe Rolling).
///
/// `SP_MOCKER_MANIFEST` overrides; otherwise `sp-llm/manifests/mocker-v1.yaml`.
pub fn mocker_v1_manifest_path() -> PathBuf {
    if let Ok(path) = std::env::var("SP_MOCKER_MANIFEST") {
        return PathBuf::from(path);
    }
    sp_llm_manifest_path("mocker-v1.yaml")
}
