//! Self-contained promotion YAML fixtures for integration and unit tests.
//!
//! Product profiles are SSOT in softprobe/sp-llm. thelake tests simulate the
//! attribute→column cases locally so CI does not need a sibling checkout.

/// Simulated LLM generation `telemetry_columns` manifest.
pub const LLM_GENERATION_V1_YAML: &str =
    include_str!("../fixtures/promotion/llm_generation_v1.yaml");

/// Simulated Rolling / mocker `telemetry_columns` manifest.
pub const MOCKER_ROLLING_V1_YAML: &str =
    include_str!("../fixtures/promotion/mocker_rolling_v1.yaml");
