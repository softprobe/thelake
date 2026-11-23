#!/usr/bin/env bash
# Send OTLP traces and logs to a hosted thelake (or any OTLP/HTTP
# endpoint) using opentelemetry-collector-contrib's telemetrygen.
#
# Requires: telemetrygen on PATH, e.g.
#   go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
# Or run the same image interactively and invoke telemetrygen inside:
#   docker run --rm -e INGESTION_KEY -e OTLP_ENDPOINT ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest traces --help
#
# Authentication: hosted runtimes expect Authorization: Bearer <Agent Ingestion Key>.
#   export INGESTION_KEY="spk_..."
#   Optional: source repo-root .env (this script loads ../../.env if present).
#
# Environment:
#   INGESTION_KEY     Agent Ingestion Key for OTLP writes (required)
#   OTLP_ENDPOINT     host:port for OTLP HTTP/TLS (default: runtime.softprobe.dev:443)
#   TELEMETRYGEN_CMD  override binary (default: telemetrygen)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

OTLP_ENDPOINT="${OTLP_ENDPOINT:-runtime.softprobe.dev:443}"
TELEMETRYGEN_CMD="${TELEMETRYGEN_CMD:-telemetrygen}"

usage() {
  cat <<'EOF'
Usage: telemetrygen_hosted.sh <traces|logs|all> [extra telemetrygen args...]

Examples:
  INGESTION_KEY="$KEY" ./telemetrygen_hosted.sh traces --traces 5
  # Or rely on INGESTION_KEY from repo-root .env after sourcing:
  ./telemetrygen_hosted.sh all
  OTLP_ENDPOINT=localhost:8090 INGESTION_KEY="$KEY" ./telemetrygen_hosted.sh all

Subcommands:
  traces   OTLP HTTP traces -> /v1/traces
  logs     OTLP HTTP logs    -> /v1/logs
  all      run traces, then logs (small batches)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SUB="${1:-}"
shift || true

if [[ -z "${SUB}" ]]; then
  usage >&2
  exit 1
fi

if [[ -z "${INGESTION_KEY:-}" ]]; then
  echo "error: set INGESTION_KEY (Agent Ingestion Key for OTLP writes)" >&2
  exit 1
fi

# telemetrygen expects --otlp-header in the form key="value" (see telemetrygen traces --help).
AUTH_HEADER="Authorization=\"Bearer ${INGESTION_KEY}\""

# Subcommand (traces|logs) must be the first argument after the binary (cobra).
otlp_flags=(
  --otlp-http
  --otlp-endpoint "${OTLP_ENDPOINT}"
  --otlp-header "${AUTH_HEADER}"
)

run_traces() {
  "${TELEMETRYGEN_CMD}" traces "${otlp_flags[@]}" --otlp-http-url-path /v1/traces "$@"
}

run_logs() {
  "${TELEMETRYGEN_CMD}" logs "${otlp_flags[@]}" --otlp-http-url-path /v1/logs "$@"
}

case "${SUB}" in
  traces)
    run_traces "$@"
    ;;
  logs)
    run_logs "$@"
    ;;
  all)
    run_traces --traces 300 --workers 10 "$@"
    run_logs --logs 300 --workers 10 "$@"
    ;;
  *)
    echo "error: unknown subcommand: ${SUB}" >&2
    usage >&2
    exit 1
    ;;
esac
