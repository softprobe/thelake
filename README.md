# Softprobe Runtime

> **The durable evidence foundation for production AI.** Softprobe preserves
> AI traces and recordings as customer-controlled data assets, directly
> queryable with standard SQL and reusable across investigation, evaluation,
> regression, governance, and continuous-improvement workflows.

Rust runtime for authenticated OTLP ingestion, tenant-scoped DuckLake storage,
DuckDB queries, and telemetry search.

Traditional software telemetry is often retained only for a short incident
window. AI traces have lasting value: today's production recording can become
tomorrow's evaluation case, regression test, audit evidence, or improvement
dataset. Softprobe keeps that evidence open and durable, while MAP attributes
and tenant-controlled column promotion provide workload-specific query paths
without discarding the original context.

See the
[product and competitive positioning](docs/positioning.md) for the strategy
and technical rationale.

## Architecture

DuckLake is the only durable telemetry backend:

```text
OTLP HTTP/gRPC
  -> tenant-bound runtime
  -> Arrow + temporary Parquet
  -> DuckLake transaction
  -> PostgreSQL DuckLake catalog
  -> inlined rows or Parquet under data_path
```

Ingest defaults to flush-through (`ingest.flush_interval_seconds: 0`): one OTLP
request becomes one DuckLake commit. Set `flush_interval_seconds` > 0 for optional
soft coalesce: OTLP acks as soon as rows are buffered; a background timer flushes
to DuckLake. Acknowledged but unflushed rows are held in memory until commit.

Start with the [documentation index](docs/README.md) for architecture,
operations, instrumentation, compatibility, and performance guidance.

## Local development

Prerequisites:

- Rust toolchain
- Docker and Docker Compose
- a dynamic DuckDB library (`DUCKDB_DOWNLOAD_LIB=1` lets the build fetch it)

Start MinIO and DuckLake PostgreSQL:

```bash
make setup
```

Build and run checks (cargo cache under `~/.cache/thelake`):

```bash
make doctor
make build
make test          # unit + lightweight
make test-e2e      # needs setup
make ci            # fmt + lint + test + test-e2e
```

Stop local infrastructure:

```bash
make teardown
```

`make ci` is the pre-merge gate. Performance is `make test-perf` (manual / release).

GitHub Actions (self-hosted Linux; Make-only; no Actions cargo/`target` cache):

- `.github/workflows/ci.yml` — on push/PR: `make doctor` → `setup` → `ci`. Warm SLO ≤ 18m.
- `.github/workflows/performance.yml` — **manual** only: `make test-perf`
  (`PERF_SUITE=all|latency|concurrency|stability`, `PERF_TARGET_MS=1000`). Warm SLO ≤ 8m.
- `.github/workflows/release.yml` — on GitHub Release: `make release`
  (`test-perf` + `build-release` + `publish`, `--release`; PR already ran `ci`). Warm SLO ≤ 25m.

## Run

```bash
export CONFIG_FILE=config.yaml
export SOFTPROBE_LOCAL_ANONYMOUS=1
export THELAKE_DEFAULT_WORKSPACE_ID=550e8400-e29b-41d4-a716-446655440000
make run
```

Open the session and trace UI at `http://127.0.0.1:8090/explorer/`. Local
anonymous mode binds allowed ingest/query/score requests to this one workspace
and ignores caller-provided tenant selectors. Anyone who can reach the listener
can read and write that workspace. Leave this mode disabled when the listener
is reachable by untrusted users; external assertion/Bearer auth remains the
default.

Defaults:

- HTTP: `0.0.0.0:8090`
- OTLP/gRPC traces: `0.0.0.0:4317`
- config file: `config.yaml`

Set `SOFTPROBE_GRPC_DISABLE=1` to disable the gRPC listener.

## Configuration

The canonical example is [`config.yaml`](config.yaml). The active storage
section is `ducklake`:

```yaml
ducklake:
  metadata_path: "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake"
  data_path: "./warehouse/ducklake/data/"
  catalog_alias: "softprobe"
  metadata_schema: "softprobe"
  data_inlining_row_limit: 500
  writer_pool_size: 4
```

YAML holds non-secret settings only. The top-level sections are `server`,
`object_store` (`region` / optional `endpoint`), `query`, `maintenance`,
`async_jobs`, and `ducklake`. Unknown or legacy keys are rejected. Object
storage credentials are never stored in YAML; resolve them from the
environment:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` [/ `AWS_SESSION_TOKEN`]: `s3://`
  paths (MinIO, R2, AWS)
- `GCS_HMAC_ACCESS_KEY_ID` / `GCS_HMAC_SECRET` (or `GCP_HMAC_*`): `gs://` paths

Supported direct environment overrides are:

- `CONFIG_FILE`
- `PORT`
- `S3_REGION`
- `SOFTPROBE_MAX_HTTP_BODY_BYTES`

The runtime also uses deployment variables such as `SOFTPROBE_AUTH_URL`,
`SOFTPROBE_LISTEN_ADDR`, and `OTEL_GRPC_PORT`.

For `gs://` DuckLake paths, DuckDB uses GCS HMAC interoperability credentials:
`GCS_HMAC_ACCESS_KEY_ID` and `GCS_HMAC_SECRET` (or their `GCP_HMAC_*`
aliases).

## Main HTTP endpoints

Health and discovery:

- `GET /health`
- `GET /ready`
- `GET /openapi.json`
- `GET /swagger`

OTLP ingestion (product signals: traces + logs):

- `POST /v1/traces`
- `POST /v1/logs`

Evaluations:

- `POST /v1/scores`
- `GET|POST /v1/score-configs`

Queries:

- `POST /v1/query/sql` (internal/debug SQL surface)
- `POST /v1/sessions/search`
- `GET /v1/sessions/{session_id}`
- `GET /v1/sessions/{session_id}/recording` (web session replay batches)
- `POST /v1/sessions/summary/rebuild`
- `POST /v1/spans/search`
- `GET /v1/spans/{span_id}`
- `GET /v1/traces/{trace_id}`
- `GET /v1/fields`
- `GET /v1/fields/{field}/values`
- `GET /v1/data/ducklake-connection`

Control-plane routes also cover tenant provisioning and promotions.
`/v1/*` operational routes require bearer authentication
(`OPTIONS /v1/*` is exempt for browser CORS preflight); tenant provisioning
validates its admin bearer inside the handler.

Web session recording contract:
[`docs/instrumentation_guide.md`](docs/instrumentation_guide.md#web-session-recording-rrweb)
and the Softprobe LLM
[web session replay guide](https://github.com/softprobe/sp-llm/blob/main/docs/web-session-replay.md).

The HTTP product contract is
[`docs/ingestion-openapi.yaml`](docs/ingestion-openapi.yaml), served by a
running process as [`GET /openapi.json`](http://localhost:8090/openapi.json)
and browsable at [`GET /swagger`](http://localhost:8090/swagger). Schema
promotion semantics are in [`docs/promotion.md`](docs/promotion.md).

## Query DuckLake locally

```bash
make duckdb-shell
```

This renders the configured DuckLake ATTACH statement, performs a `SELECT 1`
smoke, and starts DuckDB. See
[`docs/adhoc-duckdb-ducklake.md`](docs/adhoc-duckdb-ducklake.md).

## Instrumentation and promotion

HTTP bodies are captured from `http.request` and `http.response` span events.
When those event fields are absent, the runtime accepts equivalent span
attributes, including OBI `.content` body keys. Business identifiers are
explicit searchable `sp.*` span attributes set by the application — Softprobe
does not invent them.

- Instrumentation: [`docs/instrumentation_guide.md`](docs/instrumentation_guide.md)
- Schema promotion (explicit manifests for declared `sp.*` and other sources):
  [`docs/promotion.md`](docs/promotion.md)

## Maintenance

The runtime schedules DuckLake-native maintenance for every configured tenant
scope:

- merge adjacent data files;
- expire old snapshots;
- clean old files.

Settings are under `maintenance` (and `async_jobs` for lease TTL/heartbeat) in
`config.yaml`.

## Publish Docker image

`make build-release` builds the Explorer assets, embeds them in the release
binary, and stages the binary and runtime dependencies in `dist/`. The
Dockerfile is packaging-only (`COPY dist/…`); it never runs Node or Cargo.
Cache lives at `~/.cache/thelake`.

Official path: GitHub Release → `.github/workflows/release.yml` → `make release`
(`test-perf` + unconditional `build-release` + `publish` under `--release`).
Images push to public Docker Hub **`softprobe/thelake:<tag>`** (and `:latest` for
non-prerelease). Auth: Actions secret `DOCKER_HUB_PASSWORD` (username `softprobe`).

PR CI (`make ci`, dev profile) does not build `dist/`.

Local/emergency image push: `docker login` then
`make build-release && make publish TAG=vX.Y.Z`
(on Mac, `TARGET_PLATFORM=linux/amd64 make build-release` re-enters the same
Make recipe in a linux/amd64 container). `publish` refuses incomplete `dist/`.
Optional BuildKit registry cache (`softprobe/thelake:buildcache`) speeds base
layers only — do not deploy `:buildcache` as a runtime image.

## Website

The open-source landing page is served at `/` by the runtime (same host as the
API). Source: [`website/`](website/). Product URL: https://thelake.softprobe.ai/

Crawl / LLM discovery surface (also embedded):

- [`/robots.txt`](https://thelake.softprobe.ai/robots.txt)
- [`/sitemap.xml`](https://thelake.softprobe.ai/sitemap.xml)
- [`/llms.txt`](https://thelake.softprobe.ai/llms.txt)

## License

Apache-2.0 — see [`License.txt`](License.txt).
