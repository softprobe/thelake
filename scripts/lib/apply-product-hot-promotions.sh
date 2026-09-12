#!/usr/bin/env bash
# Apply Softprobe product-hot telemetry_columns manifests (metrics + traces + logs).
# Usage: apply_product_hot_promotions <base_url> <bearer_token>
# Relies on ROOT being set to the thelake repo root (caller sets it).
#
# Note on metrics-prom-hot-labels.yaml + skinny metric_samples:
# Apply ALTERs nullable hot-label columns onto metric_samples. Live Prom resolution
# uses metric_series.labels / postings; sample-column prefer paths are for
# compatibility SQL and remain safe (COALESCE with bag/labels). Failure to apply
# is non-fatal so demos still come up.

apply_one_promotion_manifest() {
  local base_url="${1:?base_url}"
  local token="${2:?bearer_token}"
  local manifest="${3:?manifest_path}"
  local label="${4:?label}"

  if [[ ! -f "$manifest" ]]; then
    echo "ERROR: missing promotion manifest: $manifest" >&2
    return 1
  fi
  local yaml
  yaml="$(cat "$manifest")"
  local payload
  payload="$(MANIFEST_YAML="$yaml" python3 - <<'PY'
import json, os
print(json.dumps({"manifestYaml": os.environ["MANIFEST_YAML"]}))
PY
)"
  echo "==> applying ${label} promotion"
  local resp http
  local tmp
  tmp="$(mktemp -t thelake-promo-apply.XXXXXX.json)"
  http="$(curl -sS --max-time 60 -o "$tmp" -w '%{http_code}' \
    -X POST "${base_url%/}/v1/promotions/apply" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d "$payload" || true)"
  if [[ "$http" != "200" && "$http" != "201" ]]; then
    echo "WARN: promotions/apply (${label}) returned HTTP ${http:-curl-fail} — continuing" >&2
    cat "$tmp" >&2 || true
    rm -f "$tmp"
    return 0
  fi
  echo "==> ${label} promotion applied (HTTP $http)"
  rm -f "$tmp"
}

apply_product_hot_promotions() {
  local base_url="${1:?base_url}"
  local token="${2:?bearer_token}"
  apply_one_promotion_manifest \
    "$base_url" "$token" \
    "${ROOT}/docs/promotion/metrics-prom-hot-labels.yaml" \
    "metrics Prom hot-labels"
  apply_one_promotion_manifest \
    "$base_url" "$token" \
    "${ROOT}/docs/promotion/traces-query-hot-attrs.yaml" \
    "traces query-hot attrs"
  apply_one_promotion_manifest \
    "$base_url" "$token" \
    "${ROOT}/docs/promotion/logs-query-hot-attrs.yaml" \
    "logs query-hot attrs"
}

# Backward-compatible alias: metrics-only callers now get full product-hot set.
apply_prom_hot_labels() {
  apply_product_hot_promotions "$@"
}
