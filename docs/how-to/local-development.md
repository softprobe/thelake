# Local Lisa development

The interactive development environment runs Explorer, TheLake, and Lisa in
the repository's root Compose project. Lisa uses a PostgreSQL policy and
activity database on the same local Postgres service as DuckLake. This is the
only persistent local environment for chatting with Lisa.

## Start the stack

Copy `.env.example` to `.env`, then set `GEMINI_API_KEY` or `GOOGLE_API_KEY`.
OpenAI credentials are only needed if you opt into scheduled policy triage.

```bash
cp .env.example .env
# Add your model key to .env, then:
make dev-up
```

The first start builds the Linux TheLake package and service images, then creates Lisa's database, dedicated
writer and reader roles, and workspace binding. `make dev-up` generates missing
local tokens and database passwords in `.env` without replacing values already
there. Keep `.env` private.

Open the Explorer URL printed by `make dev-up`, then select **Lisa** to chat.
The local anonymous identity is `local`, and the dev stack configures it as a
Lisa owner so you can exercise policy proposal confirmations through chat.
The UI, API, and local Postgres ports bind to loopback. The default Postgres
port is `55432` and Lisa's API port is `18083`, so they can coexist with older
local stacks that used the standard Postgres port or Lisa port `18082`. These
ports remain configurable in `.env`. Browser chat uses the fixed local
workspace. Policies, activities, conversations, and telemetry persist in the
Compose volumes and the `warehouse/` data directory across restarts. The
integrated stack uses its own `softprobe_dev` catalog schema and object-store
prefix; the existing `softprobe` catalog is left untouched because its stored
schema predates the current workspace-scoped catalog contract.

To populate Explorer with synthetic telemetry for browsing, run:

```bash
OTLP_ENDPOINT=http://127.0.0.1:8090 make generate-telemetry
```

This sends a small batch of sample traces and logs to the local stack. The
generator defaults to the local OTLP/HTTP endpoint on port `8090` and accepts
`OTLP_ENDPOINT` when the service uses another address.

Scheduled QA is disabled in this local stack to prevent unattended model calls.
Set `LISA_QA_ENABLED=1` in `.env` to enable `LISA_QA_CRON`; provider usage may
incur charges. Owner-approved activity follow-ups remain enabled separately
through `LISA_ACTIVITY_FOLLOWUPS_ENABLED`.

Stop Lisa and TheLake while preserving local state and shared Postgres, MinIO,
and auth-mock services:

```bash
make dev-down
```

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
