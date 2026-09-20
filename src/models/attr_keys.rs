//! Softprobe / GenAI OTLP attribute keys — single source for ingest, SQL, tests.
//!
//! Prefer these over string literals so a typo cannot silently break promotion,
//! dirty fold, reduce, or list filters.

/// Softprobe product keys (`sp.*`).
pub mod sp {
    pub const SESSION_ID: &str = "sp.session.id";
    pub const OBSERVATION_TYPE: &str = "sp.observation.type";
    pub const USER_ID: &str = "sp.user.id";
    pub const AGENT_NAME: &str = "sp.agent.name";
    pub const COST_TOTAL: &str = "sp.cost.total";
}

/// OpenTelemetry GenAI semantic conventions used by Softprobe LLM SQL.
pub mod gen_ai {
    pub const REQUEST_MODEL: &str = "gen_ai.request.model";
    pub const PROVIDER_NAME: &str = "gen_ai.provider.name";
    pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";
    pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";
    pub const USAGE_TOTAL_TOKENS: &str = "gen_ai.usage.total_tokens";
}

/// Resource attribute keys.
pub mod resource {
    pub const SERVICE_NAME: &str = "service.name";
}

/// OTel end-user identity (lake list may COALESCE; session_summary reduce does **not**
/// promote this — stays bag-only per traces-query-hot-attrs contract).
pub mod enduser {
    pub const ID: &str = "enduser.id";
}
