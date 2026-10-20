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
DOGFOOD_GEMINI_API_KEY="$api_key" python3 - "$local_env_file" <<'PY'
from pathlib import Path
import os
import sys

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
lines = [line for line in lines if not line.startswith(("GOOGLE_API_KEY=", "GEMINI_API_KEY="))]
lines.append(f"GEMINI_API_KEY={os.environ['DOGFOOD_GEMINI_API_KEY']}")
path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
os.chmod(path, 0o600)
PY

if [[ "$(uname -s)" == "Linux" ]]; then
  export THELAKE_DOGFOOD_AGENT_NETWORK=host
  agent_api_host=127.0.0.1
  agent_network_setting="THELAKE_DOGFOOD_AGENT_NETWORK=host "
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
    raise SystemExit("Dogfood ports must be between 1 and 65535.")
if requested and int(requested) in reserved:
    raise SystemExit(f"Port {requested} is already assigned to another dogfood service.")
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
    raise SystemExit("No free localhost port in the dogfood range; set a THELAKE_DOGFOOD_*_PORT value.")
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
THELAKE_DOGFOOD_DB_PORT="$(choose_port 55432 "${THELAKE_DOGFOOD_DB_PORT:-}")"
export THELAKE_DOGFOOD_DB_PORT
selected_ports+=("$THELAKE_DOGFOOD_DB_PORT")
THELAKE_DOGFOOD_LISA_PORT="$(choose_port 18082 "${THELAKE_DOGFOOD_LISA_PORT:-}")"
export THELAKE_DOGFOOD_LISA_PORT
selected_ports+=("$THELAKE_DOGFOOD_LISA_PORT")
THELAKE_DOGFOOD_PORT="$(choose_port 8090 "${THELAKE_DOGFOOD_PORT:-}")"
export THELAKE_DOGFOOD_PORT
export THELAKE_DOGFOOD_PROJECT_NAME="${THELAKE_DOGFOOD_PROJECT_NAME:-}"
if [[ ! "$THELAKE_DOGFOOD_PROJECT_NAME" =~ ^lisa-policy-eval-[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "Set THELAKE_DOGFOOD_PROJECT_NAME to a fresh lisa-policy-eval-* run name; Dogfood is disposable QA setup, not a development environment." >&2
  exit 1
fi
export THELAKE_DOGFOOD_INSTANCE_ID="${THELAKE_DOGFOOD_INSTANCE_ID:-$(python3 -c 'import secrets; print(secrets.token_hex(16))')}"
if [[ ! "$THELAKE_DOGFOOD_INSTANCE_ID" =~ ^[a-f0-9]{32}$ ]]; then
  echo "THELAKE_DOGFOOD_INSTANCE_ID must be a 32-character lowercase hexadecimal value." >&2
  exit 1
fi
export THELAKE_DOGFOOD_DATA_PATH="${THELAKE_DOGFOOD_DATA_PATH:-./warehouse/dogfood/data/}"
if [[ ! "$THELAKE_DOGFOOD_DATA_PATH" =~ ^\./warehouse/dogfood/[a-zA-Z0-9_-]+/?$ ]]; then
  echo "THELAKE_DOGFOOD_DATA_PATH must be a directory directly under warehouse/dogfood." >&2
  exit 1
fi
export THELAKE_DOGFOOD_ENV_FILE="${THELAKE_DOGFOOD_ENV_FILE:-warehouse/dogfood/compose.env}"
compose_env_file="$THELAKE_DOGFOOD_ENV_FILE"
if [[ ! "$compose_env_file" =~ ^warehouse/dogfood/[a-zA-Z0-9_-]+\.env$ ]]; then
  echo "THELAKE_DOGFOOD_ENV_FILE must be an env file directly under warehouse/dogfood." >&2
  exit 1
fi
if [[ "$THELAKE_DOGFOOD_PROJECT_NAME" == lisa-policy-eval-* ]]; then
  if [[ "$THELAKE_DOGFOOD_ENV_FILE" != "warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.env" || "$THELAKE_DOGFOOD_DATA_PATH" != "./warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}" ]]; then
    echo "Lisa policy evaluation project, metadata file, and data directory must use the same fresh run name." >&2
    exit 1
  fi
  if [[ -L warehouse || -L warehouse/dogfood || -L "$THELAKE_DOGFOOD_DATA_PATH" ]]; then
    echo "Refusing to use symlinked Lisa policy evaluation paths." >&2
    exit 1
  fi
  if [[ -e "$THELAKE_DOGFOOD_DATA_PATH" || -L "$compose_env_file" || -e "$compose_env_file" ]]; then
    echo "Refusing to reuse existing Lisa policy evaluation data or metadata." >&2
    exit 1
  fi
  for artifact in \
    "warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.yaml" \
    "warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.api_url" \
    "warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.lisa_api_url"; do
    if [[ -L "$artifact" || -e "$artifact" ]]; then
      echo "Refusing to reuse existing Lisa policy evaluation metadata: $artifact" >&2
      exit 1
    fi
  done
fi
if [[ "$THELAKE_DOGFOOD_PROJECT_NAME" == lisa-policy-eval-* ]]; then
  config_file="warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.yaml"
  api_url_file="warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.api_url"
  lisa_api_url_file="warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.lisa_api_url"
else
  config_file=warehouse/dogfood/config.yaml
  api_url_file=warehouse/dogfood/api_url
  lisa_api_url_file=warehouse/dogfood/lisa_api_url
fi
if [[ "$THELAKE_DOGFOOD_PROJECT_NAME" == lisa-policy-eval-* ]]; then
  existing_containers="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$THELAKE_DOGFOOD_PROJECT_NAME")"
  existing_volumes="$(docker volume ls --quiet --filter "label=com.docker.compose.project=$THELAKE_DOGFOOD_PROJECT_NAME")"
  existing_networks="$(docker network ls --quiet --filter "label=com.docker.compose.project=$THELAKE_DOGFOOD_PROJECT_NAME")"
  if [[ -n "$existing_containers$existing_volumes$existing_networks" ]]; then
    echo "Refusing to reuse existing Docker resources for Compose project $THELAKE_DOGFOOD_PROJECT_NAME." >&2
    exit 1
  fi
fi
mkdir -p "$(dirname "$compose_env_file")"
(umask 077; printf 'LISA_THELAKE_TOKEN=%s\nTHELAKE_DEFAULT_WORKSPACE_ID=%s\nLISA_QA_CRON=%s\nTHELAKE_DOGFOOD_PROJECT_NAME=%s\nTHELAKE_DOGFOOD_INSTANCE_ID=%s\nTHELAKE_DOGFOOD_DATA_PATH=%s\nTHELAKE_DOGFOOD_DB_PORT=%s\nTHELAKE_DOGFOOD_LISA_PORT=%s\nTHELAKE_DOGFOOD_PORT=%s\nLISA_DATABASE_WRITER_PASSWORD=%s\nLISA_DATABASE_READER_PASSWORD=%s\n' "$LISA_THELAKE_TOKEN" "$THELAKE_DEFAULT_WORKSPACE_ID" "$LISA_QA_CRON" "$THELAKE_DOGFOOD_PROJECT_NAME" "$THELAKE_DOGFOOD_INSTANCE_ID" "$THELAKE_DOGFOOD_DATA_PATH" "$THELAKE_DOGFOOD_DB_PORT" "$THELAKE_DOGFOOD_LISA_PORT" "$THELAKE_DOGFOOD_PORT" "$LISA_DATABASE_WRITER_PASSWORD" "$LISA_DATABASE_READER_PASSWORD" > "$compose_env_file")
chmod 600 "$compose_env_file"
compose=(docker compose --env-file "$local_env_file" --env-file "$compose_env_file" --project-name "$THELAKE_DOGFOOD_PROJECT_NAME" --file examples/lisa-dogfood/compose.yaml)
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
LISA_DATABASE_PROVISION_URL="postgresql://ducklake:ducklake@127.0.0.1:${THELAKE_DOGFOOD_DB_PORT}/lisa" \
LISA_WORKSPACE_ID="$THELAKE_DEFAULT_WORKSPACE_ID" \
LISA_DATABASE_WRITER_PASSWORD="$LISA_DATABASE_WRITER_PASSWORD" \
LISA_DATABASE_READER_PASSWORD="$LISA_DATABASE_READER_PASSWORD" \
LISA_DATABASE_RECREATE=1 \
  uv run --project lisa python lisa/scripts/provision_lisa.py

"${compose[@]}" up --build --detach

echo "Waiting for Lisa..."
for _ in $(seq 1 90); do
  if curl --silent --fail "http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/health" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl --silent --fail "http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/health" >/dev/null; then
  "${compose[@]}" logs lisa
  echo "Lisa agent did not become ready." >&2
  exit 1
fi

mkdir -p "$THELAKE_DOGFOOD_DATA_PATH"
sed -e "s#^  metadata_path: .*#  metadata_path: \"host=127.0.0.1 port=${THELAKE_DOGFOOD_DB_PORT} dbname=ducklake user=ducklake password=ducklake\"#" \
  -e "s#^  data_path: .*#  data_path: \"${THELAKE_DOGFOOD_DATA_PATH}\"#" \
  examples/lisa-dogfood/config.yaml > "$config_file"
export CONFIG_FILE="$config_file"
export SOFTPROBE_LOCAL_ANONYMOUS=1
export LISA_LOCAL_ANONYMOUS=1
export LISA_PUBLIC_URL="http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/v1"
export LISA_THELAKE_TOKEN
export SOFTPROBE_LISTEN_ADDR="127.0.0.1:${THELAKE_DOGFOOD_PORT}"
export THELAKE_API_URL="http://${SOFTPROBE_LISTEN_ADDR}"
export SOFTPROBE_GRPC_DISABLE=1
printf '%s\n' "$THELAKE_API_URL" > "$api_url_file"
printf 'http://127.0.0.1:%s\n' "$THELAKE_DOGFOOD_LISA_PORT" > "$lisa_api_url_file"

echo "Starting disposable Lisa QA stack at http://${SOFTPROBE_LISTEN_ADDR}/explorer/"
exec make run
