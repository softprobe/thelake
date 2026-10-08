#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ -z "${GOOGLE_API_KEY:-}" && -z "${GEMINI_API_KEY:-}" ]]; then
  echo "Set GOOGLE_API_KEY (or GEMINI_API_KEY) to run the online evaluator." >&2
  exit 1
fi
if [[ -z "${GOOGLE_API_KEY:-}" ]]; then
  export GOOGLE_API_KEY="$GEMINI_API_KEY"
fi
command -v docker >/dev/null || { echo "Docker is required." >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required." >&2; exit 1; }
command -v python3 >/dev/null || { echo "Python 3 is required for the sample trace command." >&2; exit 1; }
command -v make >/dev/null || { echo "Make is required to build and run theLake." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Start Docker Desktop or the Docker daemon, then retry." >&2; exit 1; }

choose_port() {
  python3 - "$1" "${2:-}" <<'PY'
import socket
import sys

requested = sys.argv[2]
ports = [int(requested)] if requested else range(int(sys.argv[1]), int(sys.argv[1]) + 20)
for port in ports:
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        print(port)
        break
else:
    raise SystemExit("No free localhost port in the quickstart range; set a THELAKE_QUICKSTART_*_PORT value.")
PY
}

if [[ ! -f target/ducklake-extension/ducklake.duckdb_extension ]]; then
  make ducklake-extension
fi

export THELAKE_EVALUATION_RUNNER_TOKEN="${THELAKE_EVALUATION_RUNNER_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')}"
export THELAKE_QUICKSTART_DB_PORT="$(choose_port 55432 "${THELAKE_QUICKSTART_DB_PORT:-}")"
export THELAKE_QUICKSTART_RUNNER_PORT="$(choose_port 18081 "${THELAKE_QUICKSTART_RUNNER_PORT:-}")"
export THELAKE_QUICKSTART_PORT="$(choose_port 8090 "${THELAKE_QUICKSTART_PORT:-}")"
compose=(docker compose --project-name thelake-quickstart --file examples/quickstart/compose.yaml)
"${compose[@]}" up --build --detach

echo "Waiting for Postgres..."
for _ in $(seq 1 60); do
  if "${compose[@]}" exec --no-TTY postgres pg_isready -U ducklake -d ducklake >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! "${compose[@]}" exec --no-TTY postgres pg_isready -U ducklake -d ducklake >/dev/null 2>&1; then
  "${compose[@]}" logs postgres
  echo "Postgres did not become ready." >&2
  exit 1
fi

echo "Waiting for the evaluation runner..."
for _ in $(seq 1 60); do
  if curl --silent --fail "http://127.0.0.1:${THELAKE_QUICKSTART_RUNNER_PORT}/health" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl --silent --fail "http://127.0.0.1:${THELAKE_QUICKSTART_RUNNER_PORT}/health" >/dev/null; then
  "${compose[@]}" logs evaluation-runner
  echo "Evaluation runner did not become ready." >&2
  exit 1
fi

mkdir -p warehouse/quickstart/data
sed "s/port=55432/port=${THELAKE_QUICKSTART_DB_PORT}/" \
  examples/quickstart/config.yaml > warehouse/quickstart/config.yaml
export CONFIG_FILE=warehouse/quickstart/config.yaml
export SOFTPROBE_LOCAL_ANONYMOUS=1
export THELAKE_DEFAULT_WORKSPACE_ID=550e8400-e29b-41d4-a716-446655440000
export THELAKE_EVALUATION_RUNNER_URL="http://127.0.0.1:${THELAKE_QUICKSTART_RUNNER_PORT}/v1/evaluate"
export SOFTPROBE_LISTEN_ADDR="127.0.0.1:${THELAKE_QUICKSTART_PORT}"
export THELAKE_API_URL="http://${SOFTPROBE_LISTEN_ADDR}"
export SOFTPROBE_GRPC_DISABLE=1
printf '%s\n' "$THELAKE_API_URL" > warehouse/quickstart/api_url

echo "Starting theLake at http://${SOFTPROBE_LISTEN_ADDR}/explorer/"
echo "Leave this terminal running. In another terminal, run:"
echo "  python3 examples/quickstart/send_sample_trace.py"
exec make run
