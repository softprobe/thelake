//! Softprobe and GenAI OTLP attribute keys, precedence slices, and canonical observation mappings.

/// Softprobe product keys and internal span constants.
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
    pub const TOOL_CALL_OPERATION: &str = "ai.toolCall";
    pub const EMBED_OPERATION_PREFIX: &str = "ai.embed";
    pub const DO_STREAM_SUFFIX: &str = ".doStream";
    pub const DO_GENERATE_SUFFIX: &str = ".doGenerate";
    pub const GENERATION_OPERATIONS: &[&str] = &[
        "ai.streamText",
        "ai.generateText",
        "ai.streamObject",
        "ai.generateObject",
    ];
}

/// Resource attribute keys.
pub mod resource {
    pub const SERVICE_NAME: &str = "service.name";
}

/// OTel end-user identity.
pub mod enduser {
    pub const ID: &str = "enduser.id";
}

/// Ordered attribute fallback keys for canonical `session_id` resolution.
pub const SESSION_ID_PRECEDENCE: &[&str] = &[
    sp::SESSION_ID,
    sp::SESSION_ID_UNDERSCORE,
    otel::SESSION_ID,
    gen_ai::CONVERSATION_ID,
    vercel_ai::SESSION_ID,
    vercel_ai::CONVERSATION_ID,
    traceloop::SESSION_ID,
];

/// Ordered attribute fallback keys for canonical `user_id` resolution.
pub const USER_ID_PRECEDENCE: &[&str] = &[
    sp::USER_ID,
    otel::USER_ID,
    enduser::ID,
    vercel_ai::USER_ID,
    traceloop::USER_ID,
];

/// Ordered attribute fallback keys for explicit `agent_name` resolution.
pub const AGENT_NAME_PRECEDENCE: &[&str] = &[gen_ai::AGENT_NAME, sp::AGENT_NAME];

/// Ordered attribute fallback keys for canonical `model_name` resolution.
pub const MODEL_NAME_PRECEDENCE: &[&str] = &[
    gen_ai::REQUEST_MODEL,
    gen_ai::RESPONSE_MODEL,
    openinference::LLM_MODEL_NAME,
    vercel_ai::MODEL_ID,
];

/// Ordered attribute fallback keys for canonical `model_provider` resolution.
pub const MODEL_PROVIDER_PRECEDENCE: &[&str] = &[
    gen_ai::PROVIDER_NAME,
    gen_ai::SYSTEM,
    openinference::LLM_PROVIDER,
    openinference::LLM_SYSTEM,
    vercel_ai::MODEL_PROVIDER,
];

/// Ordered attribute fallback keys for canonical `input_tokens` resolution.
pub const INPUT_TOKENS_PRECEDENCE: &[&str] = &[
    gen_ai::USAGE_INPUT_TOKENS,
    gen_ai::USAGE_PROMPT_TOKENS,
    openinference::TOKEN_COUNT_PROMPT,
    vercel_ai::USAGE_INPUT_TOKENS,
    vercel_ai::USAGE_PROMPT_TOKENS,
];

/// Ordered attribute fallback keys for canonical `output_tokens` resolution.
pub const OUTPUT_TOKENS_PRECEDENCE: &[&str] = &[
    gen_ai::USAGE_OUTPUT_TOKENS,
    gen_ai::USAGE_COMPLETION_TOKENS,
    openinference::TOKEN_COUNT_COMPLETION,
    vercel_ai::USAGE_OUTPUT_TOKENS,
    vercel_ai::USAGE_COMPLETION_TOKENS,
];

/// Ordered attribute fallback keys for canonical `total_tokens` resolution.
pub const TOTAL_TOKENS_PRECEDENCE: &[&str] = &[
    gen_ai::USAGE_TOTAL_TOKENS,
    openinference::TOKEN_COUNT_TOTAL,
    vercel_ai::USAGE_TOTAL_TOKENS,
];

/// Lowercase `gen_ai.operation.name` to canonical `observation_type` mapping.
pub const GEN_AI_OPERATION_TO_OBS_TYPE: &[(&str, &str)] = &[
    ("chat", "generation"),
    ("generate_content", "generation"),
    ("text_completion", "generation"),
    ("embeddings", "embedding"),
    ("execute_tool", "tool"),
    ("invoke_agent", "agent"),
    ("create_agent", "agent"),
    ("invoke_workflow", "agent"),
];

/// Uppercase `openinference.span.kind` to canonical `observation_type` mapping.
pub const OPENINFERENCE_KIND_TO_OBS_TYPE: &[(&str, &str)] = &[
    ("LLM", "generation"),
    ("EMBEDDING", "embedding"),
    ("TOOL", "tool"),
    ("AGENT", "agent"),
    ("CHAIN", "chain"),
    ("RETRIEVER", "retriever"),
    ("RERANKER", "retriever"),
    ("GUARDRAIL", "guardrail"),
    ("EVALUATOR", "evaluator"),
];

/// Lowercase `traceloop.span.kind` to canonical `observation_type` mapping.
pub const TRACELOOP_KIND_TO_OBS_TYPE: &[(&str, &str)] = &[
    ("agent", "agent"),
    ("tool", "tool"),
    ("workflow", "chain"),
    ("task", "chain"),
];
