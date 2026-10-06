# OpenAI / Gemini integration

Softprobe provides Langfuse-style OpenAI SDK wrappers that emit Softprobe
generations over OTLP.

## Python

```python
from openai import OpenAI
from softprobe import SoftprobeClient
from softprobe.openai import observe_openai

sp = SoftprobeClient(
    public_key="...",
    base_url="http://127.0.0.1:8091",
    otlp_endpoint="http://127.0.0.1:8091/v1/traces",
)
client = observe_openai(OpenAI(), softprobe_client=sp, session_id="sess-1")
client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "hi"}],
    name="my-generation",  # Softprobe-only
)
sp.force_flush()
sp.shutdown()
```

Gemini uses the OpenAI-compatible endpoint (default model
`gemini-2.5-flash`):

```python
from softprobe.openai import GEMINI_OPENAI_BASE_URL, observe_openai
client = observe_openai(
    OpenAI(api_key=gemini_key, base_url=GEMINI_OPENAI_BASE_URL),
    softprobe_client=sp,
)
```

Install extras: `pip install 'softprobe[openai]'` or `uv sync` in this workspace.

## TypeScript

```ts
import OpenAI from "openai";
import { SoftprobeClient, observeOpenAI } from "@softprobe/tracing";

const sp = new SoftprobeClient({ ... });
const client = observeOpenAI(new OpenAI(), {
  softprobeClient: sp,
  sessionId: "sess-1",
});
await client.chat.completions.create({
  model: "gpt-4o-mini",
  messages: [{ role: "user", content: "hi" }],
  name: "my-generation",
});
```

## Live E2E

Keys are loaded from `../.env` (`OPENAI_API_KEY`, `GEMINI_KEY`) or `sp-llm/.env`.
See `.env.example`.

```bash
make e2e-up
make phase3-live-llm
make phase3-live-llm-inspect
```
