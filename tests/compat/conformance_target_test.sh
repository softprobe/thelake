#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

real_plan=$(make --no-print-directory -n test-compat 2>&1)
grep -Fq 'scripts/compat/conformance.sh' <<<"$real_plan"
grep -Fq 'real) ;;' <<<"$real_plan"
grep -Fq 'mock) args+=(--mock)' <<<"$real_plan"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/compat-target-test.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

COMPAT_CONFORMANCE_MODE=mock \
COMPAT_CONFORMANCE_OUT="$tmp_dir/mock" \
	make --no-print-directory test-compat >/dev/null

test "$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).fetch("mode")' "$tmp_dir/mock/versions.json")" = mock
grep -Fq 'not service-backed compatibility evidence' "$tmp_dir/mock/NOTICE.txt"

set +e
invalid_output=$(COMPAT_CONFORMANCE_MODE=invalid make --no-print-directory test-compat 2>&1)
invalid_status=$?
set -e
test "$invalid_status" -eq 2
grep -Fq 'COMPAT_CONFORMANCE_MODE must be real or mock' <<<"$invalid_output"

echo "compatibility target regression: PASS"
