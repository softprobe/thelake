#!/usr/bin/env bash
# Backward-compatible wrapper — prefer apply-product-hot-promotions.sh.
# Usage: apply_prom_hot_labels <base_url> <bearer_token>
# Relies on ROOT being set to the thelake repo root (caller sets it).

# shellcheck source=scripts/lib/apply-product-hot-promotions.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/apply-product-hot-promotions.sh"
