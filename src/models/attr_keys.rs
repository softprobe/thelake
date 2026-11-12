//! Softprobe / GenAI OTLP attribute keys — single source for ingest, SQL, tests.
//!
//! Prefer these over string literals so a typo cannot silently break promotion,
//! dirty fold, reduce, or list filters.

/// Softprobe product keys (`sp.*`).
pub mod sp {
    pub const SESSION_ID: &str = "sp.session.id";
    pub const SESSION_ID_UNDERSCORE: &str = "sp_session_id";
    pub const OBSERVATION_TYPE: &str = "sp.observation.type";
    pub const USER_ID: &str = "sp.user.id";
    pub const AGENT_NAME: &str = "sp.agent.name";
    pub const COST_TOTAL: &str = "sp.cost.total";
    pub const HTTP_BODY_CAPTURE_SPAN_NAME: &str = "softprobe.http.body.capture";
}

/// OpenTelemetry GenAI semantic conventions.
pub mod gen_ai {
    pub const OPERATION_NAME: &str = "gen_ai.operation.name";
    pub const CONVERSATION_ID: &str = "gen_ai.conversation.id";
    pub const AGENT_NAME: &str = "gen_ai.agent.name";
    pub const REQUEST_MODEL: &str = "gen_ai.request.model";
    pub const RESPONSE_MODEL: &str = "gen_ai.response.model";
    pub const PROVIDER_NAME: &str = "gen_ai.provider.name";
    pub const SYSTEM: &str = "gen_ai.system";
    pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";
    pub const USAGE_PROMPT_TOKENS: &str = "gen_ai.usage.prompt_tokens";
    pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";
    pub const USAGE_COMPLETION_TOKENS: &str = "gen_ai.usage.completion_tokens";
    pub const USAGE_TOTAL_TOKENS: &str = "gen_ai.usage.total_tokens";
}

/// Standard OpenTelemetry session and user semantic conventions.
pub mod otel {
    pub const SESSION_ID: &str = "session.id";
    pub const USER_ID: &str = "user.id";
}

/// Arize OpenInference semantic conventions for `openinference.*` and `llm.*`.
pub mod openinference {
    pub const SPAN_KIND: &str = "openinference.span.kind";
    pub const LLM_MODEL_NAME: &str = "llm.model_name";
    pub const LLM_PROVIDER: &str = "llm.provider";
    pub const LLM_SYSTEM: &str = "llm.system";
    pub const TOKEN_COUNT_PROMPT: &str = "llm.token_count.prompt";
    pub const TOKEN_COUNT_COMPLETION: &str = "llm.token_count.completion";
    pub const TOKEN_COUNT_TOTAL: &str = "llm.token_count.total";
}

/// OpenLLMetry and Traceloop semantic conventions for `traceloop.*`.
pub mod traceloop {
    pub const SPAN_KIND: &str = "traceloop.span.kind";
    pub const ENTITY_NAME: &str = "traceloop.entity.name";
    pub const SESSION_ID: &str = "traceloop.association.properties.session_id";
    pub const USER_ID: &str = "traceloop.association.properties.user_id";
}

/// Vercel AI SDK OTel semantic conventions for `ai.*`.
pub mod vercel_ai {
    pub const OPERATION_ID: &str = "ai.operationId";
    pub const SESSION_ID: &str = "ai.telemetry.metadata.sessionId";
    pub const CONVERSATION_ID: &str = "ai.telemetry.metadata.conversationId";
    pub const USER_ID: &str = "ai.telemetry.metadata.userId";
    pub const MODEL_ID: &str = "ai.model.id";
    pub const MODEL_PROVIDER: &str = "ai.model.provider";
    pub const USAGE_INPUT_TOKENS: &str = "ai.usage.inputTokens";
    pub const USAGE_PROMPT_TOKENS: &str = "ai.usage.promptTokens";
    pub const USAGE_OUTPUT_TOKENS: &str = "ai.usage.outputTokens";
    pub const USAGE_COMPLETION_TOKENS: &str = "ai.usage.completionTokens";
    pub const USAGE_TOTAL_TOKENS: &str = "ai.usage.totalTokens";
}

/// Resource attribute keys.
pub mod resource {
    pub const SERVICE_NAME: &str = "service.name";
}

/// OTel end-user identity.
pub mod enduser {
    pub const ID: &str = "enduser.id";
}
