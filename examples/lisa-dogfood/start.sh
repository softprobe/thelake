#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
for command in docker curl python3 uv; do
  command -v "$command" >/dev/null || { echo "$command is required." >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "Start Docker, then retry." >&2; exit 1; }

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
            print(value.strip().strip("\"'"), end="")
            break
PY
}

api_key="${GOOGLE_API_KEY:-${GEMINI_API_KEY:-}}"
if [[ -z "$api_key" ]]; then api_key="$(read_local_env GOOGLE_API_KEY)"; fi
if [[ -z "$api_key" ]]; then api_key="$(read_local_env GEMINI_API_KEY)"; fi
if [[ -z "$api_key" ]]; then
  printf "Gemini API key (saved to gitignored .env): " >&2
  IFS= read -r -s api_key
  printf '\n' >&2
fi
[[ -n "$api_key" ]] || { echo "A Gemini API key is required." >&2; exit 1; }
DOGFOOD_GEMINI_API_KEY="$api_key" python3 - "$local_env_file" <<'PY'
from pathlib import Path
import os, sys
path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
lines = [line for line in lines if not line.startswith(("GOOGLE_API_KEY=", "GEMINI_API_KEY="))]
lines.append(f"GEMINI_API_KEY={os.environ['DOGFOOD_GEMINI_API_KEY']}")
path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
os.chmod(path, 0o600)
PY

export GOOGLE_API_KEY="$api_key"
export THELAKE_DEFAULT_WORKSPACE_ID="${THELAKE_DEFAULT_WORKSPACE_ID:-550e8400-e29b-41d4-a716-446655440000}"
export THELAKE_DOGFOOD_PROJECT_NAME="${THELAKE_DOGFOOD_PROJECT_NAME:-}"
[[ "$THELAKE_DOGFOOD_PROJECT_NAME" =~ ^lisa-policy-eval-[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "Set THELAKE_DOGFOOD_PROJECT_NAME to a fresh lisa-policy-eval-* run name; Dogfood is disposable QA, not a development environment." >&2
  exit 1
}
export THELAKE_DOGFOOD_INSTANCE_ID="${THELAKE_DOGFOOD_INSTANCE_ID:-$(python3 -c 'import secrets; print(secrets.token_hex(16))')}"
[[ "$THELAKE_DOGFOOD_INSTANCE_ID" =~ ^[a-f0-9]{32}$ ]] || {
  echo "THELAKE_DOGFOOD_INSTANCE_ID must be 32 lowercase hexadecimal characters." >&2
  exit 1
}
export THELAKE_DOGFOOD_DATA_PATH="${THELAKE_DOGFOOD_DATA_PATH:-./warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}}"
export THELAKE_DOGFOOD_ENV_FILE="${THELAKE_DOGFOOD_ENV_FILE:-warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.env}"
[[ "$THELAKE_DOGFOOD_DATA_PATH" == "./warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}" ]] || {
  echo "Dogfood data path must be the run-owned warehouse/dogfood/<project> directory." >&2; exit 1;
}
[[ "$THELAKE_DOGFOOD_ENV_FILE" == "warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.env" ]] || {
  echo "Dogfood environment file must be named after the run-owned project." >&2; exit 1;
}
if [[ -L warehouse || -L warehouse/dogfood || -L "$THELAKE_DOGFOOD_DATA_PATH" || -e "$THELAKE_DOGFOOD_DATA_PATH" || -e "$THELAKE_DOGFOOD_ENV_FILE" ]]; then
  echo "Refusing to reuse existing or symlinked Lisa dogfood data." >&2; exit 1
fi

select_port() {
  python3 - "$1" <<'PY'
import socket, sys
for port in range(int(sys.argv[1]), int(sys.argv[1]) + 40):
    with socket.socket() as sock:
        try: sock.bind(("127.0.0.1", port))
        except OSError: continue
        print(port); break
else: raise SystemExit("No free local port in the Lisa dogfood range.")
PY
}
export THELAKE_DOGFOOD_DB_PORT="${THELAKE_DOGFOOD_DB_PORT:-$(select_port 55432)}"
export THELAKE_DOGFOOD_LISA_PORT="${THELAKE_DOGFOOD_LISA_PORT:-$(select_port 18082)}"
export LISA_DATABASE_WRITER_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export LISA_DATABASE_READER_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export LISA_SERVICE_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"

mkdir -p warehouse/dogfood
existing="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$THELAKE_DOGFOOD_PROJECT_NAME")$(docker volume ls --quiet --filter "label=com.docker.compose.project=$THELAKE_DOGFOOD_PROJECT_NAME")$(docker network ls --quiet --filter "label=com.docker.compose.project=$THELAKE_DOGFOOD_PROJECT_NAME")"
[[ -z "$existing" ]] || { echo "Refusing to reuse Docker resources for $THELAKE_DOGFOOD_PROJECT_NAME." >&2; exit 1; }
(umask 077; cat > "$THELAKE_DOGFOOD_ENV_FILE" <<ENV
THELAKE_DEFAULT_WORKSPACE_ID=$THELAKE_DEFAULT_WORKSPACE_ID
THELAKE_DOGFOOD_PROJECT_NAME=$THELAKE_DOGFOOD_PROJECT_NAME
THELAKE_DOGFOOD_INSTANCE_ID=$THELAKE_DOGFOOD_INSTANCE_ID
THELAKE_DOGFOOD_DATA_PATH=$THELAKE_DOGFOOD_DATA_PATH
THELAKE_DOGFOOD_DB_PORT=$THELAKE_DOGFOOD_DB_PORT
THELAKE_DOGFOOD_LISA_PORT=$THELAKE_DOGFOOD_LISA_PORT
LISA_DATABASE_WRITER_PASSWORD=$LISA_DATABASE_WRITER_PASSWORD
LISA_DATABASE_READER_PASSWORD=$LISA_DATABASE_READER_PASSWORD
LISA_SERVICE_TOKEN=$LISA_SERVICE_TOKEN
ENV
)
chmod 600 "$THELAKE_DOGFOOD_ENV_FILE"
compose=(docker compose --env-file "$local_env_file" --env-file "$THELAKE_DOGFOOD_ENV_FILE" --project-name "$THELAKE_DOGFOOD_PROJECT_NAME" --file examples/lisa-dogfood/compose.yaml)
"${compose[@]}" up --build --detach postgres

echo "Waiting for Lisa's PostgreSQL..."
for _ in $(seq 1 60); do
  if "${compose[@]}" exec --no-TTY postgres pg_isready -U ducklake -d postgres >/dev/null 2>&1; then break; fi
  sleep 1
done
"${compose[@]}" exec --no-TTY postgres pg_isready -U ducklake -d postgres >/dev/null 2>&1 || {
  "${compose[@]}" logs postgres; echo "PostgreSQL did not become ready." >&2; exit 1;
}

LISA_DATABASE_PROVISION_URL="postgresql://ducklake:ducklake@127.0.0.1:${THELAKE_DOGFOOD_DB_PORT}/lisa" \
LISA_WORKSPACE_ID="$THELAKE_DEFAULT_WORKSPACE_ID" \
LISA_DATABASE_WRITER_PASSWORD="$LISA_DATABASE_WRITER_PASSWORD" \
LISA_DATABASE_READER_PASSWORD="$LISA_DATABASE_READER_PASSWORD" \
LISA_DATABASE_RECREATE=1 uv run --project lisa python lisa/scripts/provision_lisa.py
"${compose[@]}" up --build --detach lisa
for _ in $(seq 1 90); do
  if curl --silent --fail "http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/health" >/dev/null; then break; fi
  sleep 1
done
curl --silent --fail "http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/health" >/dev/null || {
  "${compose[@]}" logs lisa; echo "Lisa did not become ready." >&2; exit 1;
}
mkdir -p "$THELAKE_DOGFOOD_DATA_PATH"
printf 'http://127.0.0.1:%s\n' "$THELAKE_DOGFOOD_LISA_PORT" > "warehouse/dogfood/${THELAKE_DOGFOOD_PROJECT_NAME}.lisa_api_url"
echo "Standalone Lisa dogfood is ready: http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/lisa/"
echo "Push sessions to http://127.0.0.1:${THELAKE_DOGFOOD_LISA_PORT}/internal/v1/session-traces"
