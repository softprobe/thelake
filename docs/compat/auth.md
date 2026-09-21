# Compatibility authentication and tenant isolation

**Status:** Phase 0 contract (updated for Softprobe assertion)  
**Last updated:** 2026-09-21

## Canonical identity

Preferred Softprobe identity for Explorer and edge-proxied traffic is the
dedicated assertion header ([sp-llm#39](https://github.com/softprobe/sp-llm/issues/39)):

```http
X-Softprobe-Assertion: <jwt>
```

thelake verifies HS256 (`SOFTPROBE_ASSERTION_HMAC_SECRET` /
`ASSERTION_HMAC_SECRET`), then binds DuckLake scope from claim **`tenant_key`**
(Softprobe `tenants.tenant_id` string). Assertion traffic does **not** call the
Softprobe auth API-key validate service.

Optional assertion claims **`agent_id`** / **`agent_name`** (minted for agent API
keys) are stamped onto every ingested trace and log row. Browser/session JWTs
without those claims leave the columns NULL. `POST /v1/llm/sessions/search`
can filter by `agent_name`, derived as persisted column, then `sp.agent.name`,
then the agent span name.

### Legacy Bearer API key

Machine clients that have not yet migrated (Grafana datasources, some OTLP
ingest paths) may still send:

```http
Authorization: Bearer <softprobe-api-key-or-assertion-jwt>
```

Resolution order when `X-Softprobe-Assertion` is absent:

1. If the Bearer token is a Softprobe assertion JWT (HS256), verify it and bind
   DuckLake scope from `tenant_key` (Explorer workspace ingest keys use this).
2. Else if **`SOFTPROBE_DEFAULT_TENANT_KEY`** (alias
   `THELAKE_DEFAULT_TENANT_KEY`) is set to a non-empty DuckLake `scope_id` /
   `tenant_key` (must not be `thelake-ops`): require a non-empty Bearer, then
   bind that default lake **without** calling the Softprobe auth service.
   Use this to route legacy direct-to-thelake OTLP clients onto a known MAP-ready
   workspace lake while assertion migration completes.
3. Otherwise resolve through `SOFTPROBE_AUTH_URL` (legacy Softprobe API keys).

When both the assertion header and Authorization are present, **assertion wins**.

Default-lake fallback still requires `Authorization: Bearer …` (any non-empty
token). It does **not** open anonymous ingest. Prefer assertion or Explorer
gateway (`/api/thelake`) for new clients; keep the default key temporary and
tenant-specific.

Admin provisioning stays admin-key only: `POST /v1/tenants` with
`SOFTPROBE_ADMIN_API_KEY`.

## Tenant constitution

Operational and compatibility handlers **must not** accept `tenant_id` (or
equivalent) from query parameters or request bodies. Tenant scope comes only
from the authenticated context established by middleware (`tenant_key` from
assertion, or auth-service `tenantId` from Bearer).

## Protocol scope headers

Some Grafana/Loki/Tempo clients also send tenant scope headers. Softprobe treats
them as **informational consistency checks**, never as the source of truth.

| Protocol | Header | Behavior |
|----------|--------|----------|
| Loki | `X-Scope-OrgID` | If present and non-empty, **must equal** authenticated `tenant_id`; otherwise `403` |
| Tempo | `X-Scope-OrgID` (same convention) | If present and non-empty, **must equal** authenticated `tenant_id`; otherwise `403` |

Missing scope headers are allowed when auth succeeded: the authenticated
tenant is used.

## Self-monitoring and reserved tenant id

When `self_monitoring.enabled` is true, thelake exports process Meter
instruments via the standard OTLP metrics exporter (`OTEL_EXPORTER_OTLP_*`).
Those process metrics are **not** stored in DuckLake and are not served by a
product Prometheus API.

Reserved tenant id `thelake-ops` is rejected by `POST /v1/tenants` and by
default-lake binding so it cannot collide with customer scopes. Customer API
keys must not resolve to `thelake-ops`.

An unauthorized caller cannot select another tenant by forging `X-Scope-OrgID`
alone — middleware still requires a valid assertion or Bearer, and a mismatched
header is denied.

## Auth outcomes

| Condition | HTTP status |
|-----------|-------------|
| Missing assertion and Authorization | `401` |
| Invalid / expired assertion | `401` |
| Assertion without `tenant_key` | `403` |
| Malformed Bearer | `401` |
| Unknown / rejected API key | `403` |
| Scope header mismatches authenticated tenant | `403` |
| Authenticated, feature not implemented | `501` + `unsupported_feature` |

## Compatibility route prefixes

The following path prefixes require the same runtime auth middleware as
`/v1/*` (CORS `OPTIONS` preflight exempt where applicable):

- `/loki/api/v1/` — Loki-compatible
- `/api/traces`, `/api/v2/traces`, `/api/search` — Tempo-compatible

Admin provisioning (`POST /v1/tenants`) remains admin-key only and is unrelated
to compatibility query routes.
