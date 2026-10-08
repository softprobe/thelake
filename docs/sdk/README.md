# Softprobe SDK contracts

These documents describe the **Softprobe client SDK** observation and score
contracts (TypeScript `@softprobe/tracing`, Python `softprobe`, and related
integrations). They are hosted in this repository next to the runtime because
thelake is the store that receives the OTLP they emit.

| Document | Topic |
|----------|--------|
| [SDK contract](sdk-contract.md) | Shared public API surface for SDK packages |
| [Instrumentation](instrumentation.md) | `sp.*` attributes and observation types |
| [Data model](data-model.md) | Observations → DuckLake `traces` rows |
| [Web session replay](web-session-replay.md) | rrweb → OTLP recording spans |
| [OpenAI / Gemini](integrations-openai.md) | Provider wrappers |
| [LangChain](integrations-langchain.md) | Callback handlers |
| [Vercel AI SDK](integrations-vercel-ai.md) | AI SDK telemetry |
| [Tool calls](integrations-tool-calls.md) | Tool definition / call / execution spans |
| [Coding agents](integrations/coding-agents.md) | Agent harness conventions |

Machine-readable schemas and fixtures: [`contracts/`](../../contracts/README.md).
Validate with `python3 scripts/validate_contracts.py` (via `make contracts-test`).

## Boundaries

- **Runtime how-to** for HTTP bodies, promotion, and DuckLake columns:
  [`../how-to/instrumentation.md`](../how-to/instrumentation.md) and
  [`../how-to/promotion.md`](../how-to/promotion.md).
- **HTTP API** for ingest and query: [`../reference/openapi.yaml`](../reference/openapi.yaml).
- SDK package implementation and release live in Softprobe SDK repositories
  (for example softprobe-js). Do not treat Make targets named `phase*` as
  thelake Makefile targets — they are not defined here.
