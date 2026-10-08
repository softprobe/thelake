<p align="center">
  <a href="https://thelake.softprobe.ai/">
    <img src="website/assets/logos/thelake.svg" alt="thelake" width="96" height="96" />
  </a>
</p>

<h1 align="center">
  <a href="https://thelake.softprobe.ai/">thelake</a>
</h1>

<p align="center">
  <a href="https://thelake.softprobe.ai/">thelake.softprobe.ai</a>
</p>

> **Open source AI evidence lake on DuckDB.** thelake preserves production AI
> traces and recordings as customer-controlled data assets — durable, SQL
> queryable, and reusable across investigation, evaluation, regression,
> governance, and continuous improvement.

Rust service for authenticated OTLP ingestion, workspace-scoped DuckLake
storage, DuckDB queries, and telemetry search. Softprobe builds hosted
products on thelake; this repository is the open-source runtime.

Traditional software telemetry is often retained only for a short incident
window. AI traces have lasting value: today's production recording can become
tomorrow's evaluation case, regression test, audit evidence, or improvement
dataset. thelake keeps that evidence open and durable. Flexible attributes
live in DuckLake `MAP` columns; workspace-scoped column promotion adds typed
query paths without discarding the original context.

Product goals: [`docs/architecture/goals.md`](docs/architecture/goals.md).
Architecture: [`docs/architecture/overview.md`](docs/architecture/overview.md).
Full index: [`docs/README.md`](docs/README.md).

## Architecture

DuckLake is the only durable telemetry backend. The catalog is PostgreSQL in
every environment:

```text
OTLP HTTP/gRPC
  -> workspace-bound runtime
  -> Arrow + temporary Parquet
  -> DuckLake transaction
  -> PostgreSQL DuckLake catalog
  -> inlined rows or Parquet under data_path
```

Ingest defaults to flush-through (`ingest.flush_interval_seconds: 0`): one OTLP
request becomes one DuckLake commit. Set `flush_interval_seconds` > 0 for
optional soft coalesce: OTLP acks as soon as rows are buffered; a background
timer flushes to DuckLake. Acknowledged but unflushed rows live only in
memory until commit (and may be lost on crash or post-ack write failure).

Customer product signals are traces and logs. Optional process self-monitoring
can export OTLP metrics to an external collector; it does not add customer
metric tables.

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

`make ci` is the pre-merge gate. Performance is `make test-perf` (manual /
release).

GitHub Actions (Make-only; cache under `~/.cache/thelake` via
`.github/actions/thelake-cache`):

- `.github/workflows/ci.yml` — on push/PR: `make doctor` → `setup` →
  `check-fmt` / `lint` / `test`, plus separate DuckLake E2E and Explorer UI
  jobs. Local pre-merge gate remains `make setup && make ci` (warm SLO ≤ 18m).
- `.github/workflows/performance.yml` — **manual** only: `make test-perf`
  (`PERF_SUITE=all|latency|concurrency`, `PERF_TARGET_MS=1000`). Warm SLO ≤ 8m.
- `.github/workflows/release.yml` — on GitHub Release: `make release`
  (`test-perf` + `build-release` + `publish`, `--release`). Warm SLO ≤ 25m.

## Run

```bash
export CONFIG_FILE=config.yaml
export SOFTPROBE_LOCAL_ANONYMOUS=1
export THELAKE_DEFAULT_WORKSPACE_ID=550e8400-e29b-41d4-a716-446655440000
make run
```

Open the session and trace UI at `http://127.0.0.1:8090/explorer/`. Local
anonymous mode (`SOFTPROBE_LOCAL_ANONYMOUS=1`) binds a fixed allowlist of
`/v1/*` data-plane routes (OTLP ingest, scores POST, span/session search,
score-config GET, and single span/trace/session GET) to
`THELAKE_DEFAULT_WORKSPACE_ID` without a bearer. Other `/v1/*` routes
(including recording, promotions, SQL, and workspaces) return 403. Anyone who
can reach the listener can exercise that allowlist. Leave this mode disabled
when the listener is reachable by untrusted users; assertion/Bearer auth
remains the default.

Defaults:

- HTTP: `0.0.0.0:8090`
- OTLP/gRPC traces: `0.0.0.0:4317`
- config file: `config.yaml`

Set `SOFTPROBE_GRPC_DISABLE=1` to disable the gRPC listener.

## Configuration

Canonical example: [`config.yaml`](config.yaml). Full key reference:
[`docs/reference/config.md`](docs/reference/config.md).

```yaml
ducklake:
  metadata_path: "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake"
  data_path: "./warehouse/ducklake/data/"
  catalog_alias: "softprobe"
  metadata_schema: "softprobe"
  extension_path: "./dist/ducklake.duckdb_extension"
  data_inlining_row_limit: 500
  writer_pool_size: 4
```

Object-store credentials are environment-only (`AWS_*` for `s3://`,
`GCS_HMAC_*` / `GCP_HMAC_*` for `gs://`) — not YAML keys. The DuckLake
catalog DSN in `ducklake.metadata_path` may include a Postgres password.
Common overrides: `CONFIG_FILE`, `PORT`, `SOFTPROBE_LISTEN_ADDR`,
`OTEL_GRPC_PORT`, `SOFTPROBE_AUTH_URL`.

## Main HTTP endpoints

Health: `GET /health`, `GET /ready`, `GET /openapi.json`, `GET /swagger`

Ingest: `POST /v1/traces`, `POST /v1/logs`

Evaluations: `POST /v1/scores`, `GET|POST /v1/score-configs`

Queries:

- `POST /v1/query/sql` (internal/debug)
- `POST /v1/traces/search`, `POST /v1/traces/details`, `GET /v1/traces/{trace_id}`
- `POST /v1/spans/search`, `GET /v1/spans/{span_id}`
- `POST /v1/sessions/search`, `POST /v1/sessions/details`
- `GET /v1/sessions/{session_id}`, `GET /v1/sessions/{session_id}/recording`
- `POST /v1/sessions/summary/rebuild`
- `GET /v1/fields`, `GET /v1/fields/{field}/values`
- `GET /v1/data/ducklake-connection`

Control: `POST /v1/workspaces` (admin), `GET /v1/meta`, `POST /v1/promotions/apply`

Grafana Loki/Tempo routes: [`docs/compat/`](docs/compat/README.md).

`/v1/*` requires bearer auth (`OPTIONS /v1/*` exempt for CORS); workspace
provisioning validates its admin bearer in-handler.

Contracts:

- OpenAPI: [`docs/reference/openapi.yaml`](docs/reference/openapi.yaml) →
  [`GET /openapi.json`](http://localhost:8090/openapi.json) /
  [`GET /swagger`](http://localhost:8090/swagger)
- Instrumentation: [`docs/how-to/instrumentation.md`](docs/how-to/instrumentation.md)
- Web recording: [how-to § recording](docs/how-to/instrumentation.md#web-session-recording-rrweb),
  [SDK guide](docs/sdk/web-session-replay.md)
- Promotion: [`docs/how-to/promotion.md`](docs/how-to/promotion.md)
- Attributes: [`docs/architecture/attribute-storage.md`](docs/architecture/attribute-storage.md)

## Query DuckLake locally

```bash
make duckdb-shell
```

See [`docs/how-to/adhoc-duckdb.md`](docs/how-to/adhoc-duckdb.md).

## Maintenance

thelake schedules DuckLake-native maintenance once per physical DuckLake scope
(shared warehouses are deduped):

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
Images push to public Docker Hub **`softprobe/thelake:<tag>`** (and `:latest`
for non-prerelease). Auth: Actions secret `DOCKER_HUB_PASSWORD` (username
`softprobe`).

PR Actions (`.github/workflows/ci.yml`, dev profile) do not build `dist/`.

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
