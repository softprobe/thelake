#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required." >&2; exit 1; }
command -v python3 >/dev/null || { echo "Python 3 is required to select local ports." >&2; exit 1; }
command -v make >/dev/null || { echo "Make is required to build and run theLake." >&2; exit 1; }
command -v uv >/dev/null || { echo "uv is required to provision Lisa's local database." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Start Docker Desktop or the Docker daemon, then retry." >&2; exit 1; }

local_env_file=.env
read_local_env() {
  python3 - "$local_env_file" "$1" <<'PY'
from pathlib import Path
import sys

path, requested = Path(sys.argv[1]), sys.argv[2]
if path.exists():
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == requested:
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '\"'):
                value = value[1:-1]
            print(value, end="")
            break
PY
}

api_key="${GOOGLE_API_KEY:-${GEMINI_API_KEY:-}}"
if [[ -z "$api_key" ]]; then
  api_key="$(read_local_env GOOGLE_API_KEY)"
fi
if [[ -z "$api_key" ]]; then
  api_key="$(read_local_env GEMINI_API_KEY)"
fi
if [[ -z "$api_key" ]]; then
  printf "Gemini API key (saved to gitignored .env): " >&2
  IFS= read -r -s api_key
  printf '\n' >&2
fi
if [[ -z "$api_key" ]]; then
  echo "A Gemini API key is required for Lisa and the sample agent." >&2
  exit 1
fi
export GOOGLE_API_KEY="$api_key"
QUICKSTART_GEMINI_API_KEY="$api_key" python3 - "$local_env_file" <<'PY'
from pathlib import Path
import os
import sys

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
lines = [line for line in lines if not line.startswith(("GOOGLE_API_KEY=", "GEMINI_API_KEY="))]
lines.append(f"GEMINI_API_KEY={os.environ['QUICKSTART_GEMINI_API_KEY']}")
path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
os.chmod(path, 0o600)
PY

if [[ "$(uname -s)" == "Linux" ]]; then
  export THELAKE_QUICKSTART_AGENT_NETWORK=host
  agent_api_host=127.0.0.1
  agent_network_setting="THELAKE_QUICKSTART_AGENT_NETWORK=host "
else
  agent_api_host=host.docker.internal
  agent_network_setting=""
fi

selected_ports=()
choose_port() {
  local reserved_ports
  reserved_ports="$(IFS=,; echo "${selected_ports[*]-}")"
  python3 - "$1" "${2:-}" "$reserved_ports" <<'PY'
import socket
import sys

requested = sys.argv[2]
reserved = {int(port) for port in sys.argv[3].split(",") if port}
ports = [int(requested)] if requested else range(int(sys.argv[1]), int(sys.argv[1]) + 20)
if requested and not 1 <= int(requested) <= 65535:
    raise SystemExit("Quickstart ports must be between 1 and 65535.")
if requested and int(requested) in reserved:
    raise SystemExit(f"Port {requested} is already assigned to another quickstart service.")
for port in ports:
    if port in reserved:
        continue
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

export LISA_THELAKE_TOKEN="${LISA_THELAKE_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')}"
if [[ ! "$LISA_THELAKE_TOKEN" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "LISA_THELAKE_TOKEN must contain only letters, numbers, dots, underscores, or hyphens." >&2
  exit 1
fi
export THELAKE_DEFAULT_WORKSPACE_ID="${THELAKE_DEFAULT_WORKSPACE_ID:-550e8400-e29b-41d4-a716-446655440000}"
export LISA_QA_CRON="${LISA_QA_CRON:-0 * * * *}"
LISA_DATABASE_WRITER_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
LISA_DATABASE_READER_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
THELAKE_QUICKSTART_DB_PORT="$(choose_port 55432 "${THELAKE_QUICKSTART_DB_PORT:-}")"
export THELAKE_QUICKSTART_DB_PORT
selected_ports+=("$THELAKE_QUICKSTART_DB_PORT")
THELAKE_QUICKSTART_LISA_PORT="$(choose_port 18082 "${THELAKE_QUICKSTART_LISA_PORT:-}")"
export THELAKE_QUICKSTART_LISA_PORT
selected_ports+=("$THELAKE_QUICKSTART_LISA_PORT")
THELAKE_QUICKSTART_PORT="$(choose_port 8090 "${THELAKE_QUICKSTART_PORT:-}")"
export THELAKE_QUICKSTART_PORT
export THELAKE_QUICKSTART_PROJECT_NAME="${THELAKE_QUICKSTART_PROJECT_NAME:-thelake-quickstart}"
if [[ ! "$THELAKE_QUICKSTART_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "THELAKE_QUICKSTART_PROJECT_NAME must start with a lowercase letter or number and contain only lowercase letters, numbers, underscores, or hyphens." >&2
  exit 1
fi
export THELAKE_QUICKSTART_DATA_PATH="${THELAKE_QUICKSTART_DATA_PATH:-./warehouse/quickstart/data/}"
if [[ ! "$THELAKE_QUICKSTART_DATA_PATH" =~ ^\./warehouse/quickstart/[a-zA-Z0-9_-]+/?$ ]]; then
  echo "THELAKE_QUICKSTART_DATA_PATH must be a directory directly under warehouse/quickstart." >&2
  exit 1
fi
compose_env_file=warehouse/quickstart/compose.env
mkdir -p "$(dirname "$compose_env_file")"
(umask 077; printf 'LISA_THELAKE_TOKEN=%s\nTHELAKE_DEFAULT_WORKSPACE_ID=%s\nLISA_QA_CRON=%s\nTHELAKE_QUICKSTART_DB_PORT=%s\nTHELAKE_QUICKSTART_LISA_PORT=%s\nTHELAKE_QUICKSTART_PORT=%s\nLISA_DATABASE_WRITER_PASSWORD=%s\nLISA_DATABASE_READER_PASSWORD=%s\n' "$LISA_THELAKE_TOKEN" "$THELAKE_DEFAULT_WORKSPACE_ID" "$LISA_QA_CRON" "$THELAKE_QUICKSTART_DB_PORT" "$THELAKE_QUICKSTART_LISA_PORT" "$THELAKE_QUICKSTART_PORT" "$LISA_DATABASE_WRITER_PASSWORD" "$LISA_DATABASE_READER_PASSWORD" > "$compose_env_file")
chmod 600 "$compose_env_file"
compose=(docker compose --env-file "$local_env_file" --env-file "$compose_env_file" --project-name "$THELAKE_QUICKSTART_PROJECT_NAME" --file examples/quickstart/compose.yaml)
"${compose[@]}" up --detach postgres

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

echo "Provisioning Lisa's separate PostgreSQL database..."
LISA_DATABASE_PROVISION_URL="postgresql://ducklake:ducklake@127.0.0.1:${THELAKE_QUICKSTART_DB_PORT}/lisa" \
LISA_WORKSPACE_ID="$THELAKE_DEFAULT_WORKSPACE_ID" \
LISA_DATABASE_WRITER_PASSWORD="$LISA_DATABASE_WRITER_PASSWORD" \
LISA_DATABASE_READER_PASSWORD="$LISA_DATABASE_READER_PASSWORD" \
LISA_DATABASE_RECREATE=1 \
  uv run --project lisa python lisa/scripts/provision_lisa.py

"${compose[@]}" up --build --detach

echo "Waiting for Lisa..."
for _ in $(seq 1 90); do
  if curl --silent --fail "http://127.0.0.1:${THELAKE_QUICKSTART_LISA_PORT}/health" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl --silent --fail "http://127.0.0.1:${THELAKE_QUICKSTART_LISA_PORT}/health" >/dev/null; then
  "${compose[@]}" logs lisa
  echo "Lisa agent did not become ready." >&2
  exit 1
fi

mkdir -p "$THELAKE_QUICKSTART_DATA_PATH"
sed -e "s#^  metadata_path: .*#  metadata_path: \"host=127.0.0.1 port=${THELAKE_QUICKSTART_DB_PORT} dbname=ducklake user=ducklake password=ducklake\"#" \
  -e "s#^  data_path: .*#  data_path: \"${THELAKE_QUICKSTART_DATA_PATH}\"#" \
  examples/quickstart/config.yaml > warehouse/quickstart/config.yaml
export CONFIG_FILE=warehouse/quickstart/config.yaml
export SOFTPROBE_LOCAL_ANONYMOUS=1
export LISA_LOCAL_ANONYMOUS=1
export LISA_PUBLIC_URL="http://127.0.0.1:${THELAKE_QUICKSTART_LISA_PORT}/v1"
export LISA_THELAKE_TOKEN
export SOFTPROBE_LISTEN_ADDR="127.0.0.1:${THELAKE_QUICKSTART_PORT}"
export THELAKE_API_URL="http://${SOFTPROBE_LISTEN_ADDR}"
export SOFTPROBE_GRPC_DISABLE=1
printf '%s\n' "$THELAKE_API_URL" > warehouse/quickstart/api_url
printf 'http://127.0.0.1:%s\n' "$THELAKE_QUICKSTART_LISA_PORT" > warehouse/quickstart/lisa_api_url

echo "Starting theLake at http://${SOFTPROBE_LISTEN_ADDR}/explorer/"
echo "Leave this terminal running. In Explorer, chat with Lisa and ask her to monitor this rule:"
echo "  For quickstart-refund-agent, check refund eligibility and wait for the result before issuing a refund. Start monitoring now."
echo "Then run the sample agent to send a real trace:"
echo "  ${agent_network_setting}docker compose --env-file ${local_env_file} --env-file ${compose_env_file} --project-name ${THELAKE_QUICKSTART_PROJECT_NAME} --file examples/quickstart/compose.yaml run --build --rm refund-agent --agent-name quickstart-refund-agent --api-url http://${agent_api_host}:${THELAKE_QUICKSTART_PORT}"
exec make run
