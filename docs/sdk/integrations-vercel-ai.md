# Vercel AI SDK integration

Softprobe mirrors Langfuse’s AI SDK approach:

1. Prefer AI SDK 7 `Telemetry` via `@softprobe/vercel-ai-sdk`
2. Or use AI SDK v6 `experimental_telemetry` with Softprobe’s OTEL exporter and
   `createSoftprobeObservationAttributes` for attribute enrichment

## AI SDK 7

```bash
pnpm add ai @ai-sdk/otel @softprobe/vercel-ai-sdk @softprobe/tracing
```

Configure the Agent Ingestion Key and OTLP endpoint:

```bash
export INGESTION_KEY="<Agent Ingestion Key>"
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="https://thelake.softprobe.ai/v1/traces"
export OTEL_EXPORTER_OTLP_TRACES_HEADERS="Authorization=Bearer ${INGESTION_KEY}"
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL="http/protobuf"
```

```ts
import { registerTelemetry } from "ai";
import { SoftprobeClient } from "@softprobe/tracing";
import { SoftprobeVercelAiSdkIntegration } from "@softprobe/vercel-ai-sdk";

const sp = SoftprobeClient.fromEnv({ registerProvider: true });

registerTelemetry(
  new SoftprobeVercelAiSdkIntegration({ tracer: sp.tracer }),
);
```

`runtimeContext` keys (except prompt objects) become `sp.metadata.*`.
Prompt objects (`softprobePrompt` / `langfusePrompt`) map to
`sp.prompt.name` / `sp.prompt.version` on model/embedding spans.

## AI SDK v6 fallback

Enable telemetry on calls and export spans with Softprobe’s OTEL pipeline:

```ts
import { createSoftprobeObservationAttributes } from "@softprobe/vercel-ai-sdk";

const attrs = createSoftprobeObservationAttributes({
  spanType: "languageModel",
  runtimeContext: { route: "chat", softprobePrompt: { name: "x", version: 1 } },
});
```

Use those attributes in a custom span processor / enrich hook, or rely on the
AI SDK’s native OTEL spans plus Softprobe’s exporter attached through a shared
`NodeSDK` (`SoftprobeClient({ registerProvider: false })` + `sp.tracer`).
