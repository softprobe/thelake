# LangChain integration

Softprobe provides Langfuse-style LangChain callback handlers that emit typed
observations (`chain` / `agent` / `tool` / `retriever` / `generation`) over OTLP.

## Python

```bash
pip install 'softprobe[langchain]'
# or in this workspace:
# uv sync --package softprobe --extra langchain
```

```python
from softprobe import SoftprobeClient
from softprobe.langchain import CallbackHandler

sp = SoftprobeClient(
    public_key="e2e-token",
    base_url="http://127.0.0.1:8091",
    otlp_endpoint="http://127.0.0.1:8091/v1/traces",
)
handler = CallbackHandler(
    softprobe_client=sp,
    session_id="sess-1",
    user_id="user-1",
    tags=["langchain"],
    metadata={"feature": "support"},
)

# Pass to any LangChain / LangGraph invoke:
# chain.invoke(inputs, config={"callbacks": [handler]})
```

## TypeScript

```bash
pnpm add @softprobe/langchain @langchain/core
```

```ts
import { SoftprobeClient } from "@softprobe/tracing";
import { CallbackHandler } from "@softprobe/langchain";

const sp = new SoftprobeClient({ /* ... */ });
const handler = new CallbackHandler({
  softprobeClient: sp,
  sessionId: "sess-1",
  tags: ["langchain"],
});

// await agent.invoke(input, { callbacks: [handler] });
```

Parent/child linking uses LangChain `runId` / `parentRunId` mapped onto Softprobe
observations (explicit parent spans), so concurrent runs stay correctly nested.
`sessionId` on the handler is the product session for the whole invoke graph —
keep it constant; do not invent Softprobe `run_id` attributes from LangChain
`runId` (those ids only drive OTel parent links).
