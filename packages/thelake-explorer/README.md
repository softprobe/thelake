# @softprobe/thelake-explorer

Shared trace/session UI for thelake self-hosted deployments and Softprobe Cloud.

```tsx
import { ThelakeExplorer } from "@softprobe/thelake-explorer";
import "@softprobe/thelake-explorer/style.css";

<ThelakeExplorer config={{ apiBasePath: "/api/thelake/v1", auth: { headers: () => ({ Authorization: `Bearer ${token}` }) } }} />
```

The self-hosted standalone SPA is built from this source and served by thelake at `/explorer/`, using `/v1`. It has no Cloudflare, Supabase, or hosted-service dependency.
