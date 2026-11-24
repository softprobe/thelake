# Authentication and workspace isolation

TheLake accepts three authenticated request forms. Each request is assigned a
workspace by its credential; clients cannot select a workspace through a query
parameter or request body.

## User API access

Use the workspace's **Softprobe API Key** for read and general API access:

```http
Authorization: Bearer <SOFTPROBE_API_KEY>
```

TheLake resolves the key through the configured Softprobe auth service. The
resolved workspace ID must be a UUID. Query, session detail, score configuration,
administrative, Loki, and Tempo access follow the API key's assigned permissions.

## Agent data ingestion

Use an Agent-bound **Ingestion Key** only for data submission:

```http
Authorization: Bearer <INGESTION_KEY>
```

Explorer resolves the key to its Agent and issues a short-lived assertion scoped
to that Agent and workspace. Ingestion keys can submit traces, logs, and scores;
they cannot read sessions or invoke control, administrative, Loki, or Tempo APIs.
The Agent identity is stamped onto persisted telemetry.

When traffic passes through Explorer, the Worker may send the signed assertion
in this header instead:

```http
X-Softprobe-Assertion: <jwt>
```

TheLake verifies the assertion and uses its `workspace_id`, `agent_id`, and
`capabilities` claims. A workspace assertion has workspace access; an Agent
assertion is limited to the ingest routes.

## Workspace provisioning

`POST /v1/workspaces` uses the service-only `SOFTPROBE_ADMIN_API_KEY`. That key
is for workspace provisioning and is not a user or Agent credential.

Workspace IDs must be UUIDs on every authenticated path. The internal storage
scope `_default` is not an authentication identity. Reserved workspace ID
`thelake-ops` is rejected by provisioning.

## Protocol scope headers

Some Grafana, Loki, and Tempo clients also send tenant scope headers. These are
consistency checks only, never the source of authentication or workspace
selection.

| Protocol | Header | Behavior |
|----------|--------|----------|
| Loki | `X-Scope-OrgID` | If present, it must equal the authenticated workspace ID; otherwise `403`. |
| Tempo | `X-Scope-OrgID` | If present, it must equal the authenticated workspace ID; otherwise `403`. |

Missing scope headers are allowed after authentication succeeds.

## Self-monitoring

When `self_monitoring.enabled` is true, TheLake exports process meter
instruments through the standard OTLP metrics exporter (`OTEL_EXPORTER_OTLP_*`).
These metrics are not stored in DuckLake or served by a product Prometheus API.
