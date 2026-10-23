# Local Lisa development

The interactive development environment runs Explorer, TheLake, and Lisa in
the repository's root Compose project. Lisa and DuckLake use separate
PostgreSQL services: Lisa's database stores its policies, activities, pushed
session receipts, and durable evaluation queue. This is the only persistent
local environment for chatting with Lisa.

## Start the stack

Copy `.env.example` to `.env`, then set `GEMINI_API_KEY` or `GOOGLE_API_KEY`.
OpenAI credentials are needed for policy triage of pushed sessions.

```bash
cp .env.example .env
# Add your model key to .env, then:
make dev-up
```

The first start builds the Linux TheLake package and service images, then creates Lisa's database, dedicated
writer and reader roles, and workspace binding. `make dev-up` generates missing
local tokens and database passwords in `.env` without replacing values already
there. Keep `.env` private.

Open Explorer at `http://127.0.0.1:8090/explorer/` to browse sessions and
traces. Open Lisa at `http://127.0.0.1:18083/lisa/` to chat, inspect activities,
rules, triage, and identity. Lisa's Python service serves its UI and API on its
own port and owns its own PostgreSQL database. The local anonymous identity is `local`, and the dev stack configures it
as a Lisa owner so you can exercise readable policy creation and approval through chat.
The UI, API, and local Postgres ports bind to loopback. The default Postgres
port is `55432`, Lisa's PGMQ-enabled Postgres port is `55433`, and Lisa's API
port is `18083`. These ports remain configurable in `.env`. Browser chat uses the fixed local
workspace. Policies, activities, conversations, and telemetry persist in the
Compose volumes and the `warehouse/` data directory across restarts. The
integrated stack uses its own `softprobe_dev` catalog schema and object-store
prefix; the existing `softprobe` catalog is left untouched because its stored
schema predates the current workspace-scoped catalog contract.

To start Lisa and its dedicated Postgres/provisioner without starting TheLake,
MinIO, DuckLake Postgres, or auth-mock, run:

```bash
make dev-lisa-up
```

This starts only Lisa's Compose dependency chain. It does not stop any of those
services if they were already running.

To populate Explorer with synthetic telemetry for browsing, run:

```bash
OTLP_ENDPOINT=http://127.0.0.1:8090 make generate-telemetry
```

This sends a small batch of sample traces and logs to the local stack. The
generator defaults to the local OTLP/HTTP endpoint on port `8090` and accepts
`OTLP_ENDPOINT` when the service uses another address.

Lisa does not run scheduled QA. Evaluation starts when a complete OTLP session
is pushed to its private-network `POST /internal/v1/session-traces` endpoint.

Stop Lisa and TheLake while preserving local state, both Postgres services,
MinIO, and auth-mock:

```bash
make dev-down
```

To discard Lisa's local data and recreate its database and home directory, run:

```bash
make dev-reprovision-lisa
```

This deletes the current Compose project's `lisa-postgres-data` and `lisa-data`
volumes, then runs the normal `dev-up` workflow. It removes Lisa's local
activities, policies, queued jobs, conversations, and agent workspace. It keeps
the `.env` file and its existing credential values; normal environment
initialization may add generated settings or refresh defaults. DuckLake data,
MinIO's warehouse, and TheLake's telemetry remain intact. Lisa and the rest of
the local stack are running when the command completes.

## Rust backend development

For a host-native TheLake process while editing Rust, use the existing backend
workflow:

```bash
make setup
make build
make run
```

This host-native mode does not start Lisa. Stop the Compose stack first if it
already owns the standard HTTP, Postgres, or MinIO ports. Use `make dev-up` for
the complete chat UI.

## Automated tests and Dogfood

Browser tests and labeled Lisa evaluations use disposable, run-owned databases
and workspaces. They never connect to the persistent developer environment.
See [Tests and evaluations](test-and-eval.md), [Lisa Dogfood](policy-dogfood.md),
and the [policy evaluation guide](policy-triage-evaluation.md).
