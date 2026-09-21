# Read-after-write and queryability guarantees

**Status:** Phase 0 contract  
**Last updated:** 2026-09-21

## Ingest commit boundary

Each successful OTLP HTTP (or gRPC traces) ingest request commits through the
tenant-scoped DuckLake writer before returning success. After a `2xx` ingest
response, data is durable in that tenant's DuckLake scope.

Compatibility query adapters (Loki/Tempo) and SQL/telemetry APIs read from the
same DuckLake catalog. There is no separate application-level WAL or staged
tier that delays visibility after a successful ingest response.

## Visibility for clients

| Scenario | Guarantee |
|----------|-----------|
| Client receives `2xx` from `/v1/logs` or `/v1/traces` | Rows are committed; subsequent queries in the same tenant scope can observe them |
| Client retries an ingest after network failure without seeing a response | Duplicate rows may appear; ingest is not idempotent by payload hash |
| Query during an in-flight ingest on another connection | Uncommitted rows are not visible |

## Ordering and late data

| Case | Behavior |
|------|----------|
| Out-of-order timestamps within a batch | Stored as-is; query adapters sort deterministically for protocol responses |
| Late-arriving records (older than recent ingest) | Accepted and stored; no reject-by-staleness gate |

## Empty / invalid tenant

| Case | Behavior |
|------|----------|
| Missing Bearer on protected routes | `401` |
| Bearer resolves but tenant scope is not provisioned | Engine/scope resolution error (`4xx`/`5xx` per existing control-plane behavior) |
| Empty tenant id in authenticated context | Treated as invalid operational state; handlers must not invent a default tenant for cross-tenant data |

## Limits (defaults)

See `capability.v0.yaml` `limits` for defaults. Loki/Tempo adapters enforce
timeout and response-size caps from that manifest; exceeding enforced limits
returns a stable `limit_exceeded` Softprobe code (protocol-native envelope).
