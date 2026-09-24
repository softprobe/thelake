#!/usr/bin/env bash
# Run the complete integration suite once per DuckLake workspace-scope mode.
#
# The two jobs intentionally use the same test selector. Keeping the selector
# here prevents the shared-mode job from silently becoming a hand-maintained
# subset of the isolated-mode job.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND="${E2E_BACKEND:-local}"

run_mode() {
  local mode="$1"
  local run_id
  run_id="$(date +%Y%m%d-%H%M%S)-$$-${mode}"

  (
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
    export AWS_REGION="${AWS_REGION:-us-east-1}"
    export SPLAKE_RESET_DUCKLAKE=1
    export E2E_BACKEND="${BACKEND}"
    export WORKSPACE_SCOPE_MODE="${mode}"
    unset CONFIG_FILE

    case "${BACKEND}" in
      local)
        ;;
      gcs)
        : "${GCS_HMAC_ACCESS_KEY_ID:?Set GCS_HMAC_ACCESS_KEY_ID}"
        : "${GCS_HMAC_SECRET:?Set GCS_HMAC_SECRET}"
        export GCS_BUCKET="${GCS_BUCKET:-softprobe-datalake-ducklake}"
        export GCS_E2E_PREFIX="gs://${GCS_BUCKET}/ducklake/e2e/${run_id}/"
        echo "${mode}: GCS prefix ${GCS_E2E_PREFIX}"
        trap 'gcloud storage rm -r "${GCS_E2E_PREFIX}**" >/dev/null 2>&1 || gcloud storage rm -r "${GCS_E2E_PREFIX}" >/dev/null 2>&1 || true' EXIT
        ;;
      r2)
        if [[ -z "${E2E_DISABLE_TLS_VALIDATION:-}" ]] \
          && ! curl -sf https://www.google.com >/dev/null 2>&1; then
          export E2E_DISABLE_TLS_VALIDATION=1
        fi
        ;;
      *)
        echo "unknown E2E_BACKEND=${BACKEND} (local|gcs|r2)" >&2
        exit 1
        ;;
    esac

    echo "integration-e2e WORKSPACE_SCOPE_MODE=${mode} E2E_BACKEND=${BACKEND}"
    "${ROOT}/scripts/run-isolated-cargo-tests.sh" \
      --features integration-e2e \
      --test tests \
      --list-prefix integration::
  )
}

run_mode isolated &
isolated_pid=$!
run_mode shared &
shared_pid=$!

status=0
if ! wait "${isolated_pid}"; then
  echo "❌ isolated DuckLake mode failed" >&2
  status=1
fi
if ! wait "${shared_pid}"; then
  echo "❌ shared DuckLake mode failed" >&2
  status=1
fi

if [[ "${status}" -ne 0 ]]; then
  exit "${status}"
fi
echo "✅ DuckLake mode matrix passed: isolated + shared"
