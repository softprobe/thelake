#!/usr/bin/env bash

set -euo pipefail

# TDD contract for scripts/compat/validate-artifacts.sh.  The fixture is
# deliberately assembled at runtime so this test never leaves evidence in the
# repository and can be reused by unit, differential, and release workflows.

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/compat/validate-artifacts.sh"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/compat-artifacts-test.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT

passed=0
failed=0

pass() {
	passed=$((passed + 1))
	printf 'ok %d - %s\n' "$((passed + failed))" "$1"
}

fail() {
	failed=$((failed + 1))
	printf 'not ok %d - %s\n' "$((passed + failed))" "$1"
	printf '  error: %s\n' "$2"
}

write_fixture() {
	local root=$1
	mkdir -p "$root/suite/case-logs-001"

	python3 - "$root" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
run_id = "run-20260815T000000Z"
case_id = "logs-001"
fixture_id = "logs-basic"
protocol = "loki"
fingerprint = "a" * 64
case_dir = root / "suite" / "case-logs-001"

request = {
    "method": "GET",
    "path": "/loki/api/v1/query_range",
    "query": {"query": "{service=\"api\"}", "limit": 20},
    "headers": {"Authorization": "Bearer [REDACTED]"},
}
normalized = {
    "status": 200,
    "data": {"resultType": "streams", "result": []},
}

def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")

def metadata(extra=None):
    value = {
        "run_id": run_id,
        "case_id": case_id,
        "protocol": protocol,
        "fixture_id": fixture_id,
        "request_fingerprint": fingerprint,
    }
    if extra:
        value.update(extra)
    return value

write(root / "artifact-index.json", {
    "schema_version": "compat-artifact-index.v1",
    "run_id": run_id,
    "protocol": protocol,
    "release_evidence": True,
    "artifacts": [
        "execution-receipt.json",
        "outcome.json",
        "suite/case-logs-001/case.json",
        "suite/case-logs-001/case_provenance.json",
        "suite/case-logs-001/request.raw.json",
        "suite/case-logs-001/request.normalized.json",
        "suite/case-logs-001/softprobe.raw.json",
        "suite/case-logs-001/softprobe.normalized.json",
        "suite/case-logs-001/reference.raw.json",
        "suite/case-logs-001/reference.normalized.json",
        "suite/case-logs-001/diff.json",
        "suite/case-logs-001/outcome.json",
    ],
})
write(root / "execution-receipt.json", {
    "schema_version": "compat-execution-receipt.v1",
    "run_id": run_id,
    "protocol": protocol,
    "status": "completed",
    "outcome": "pass",
    "selected_case_ids": [case_id],
    "executed_case_ids": [case_id],
    "selected_fixture_ids": [fixture_id],
    "cases": [{
        "case_id": case_id,
        "fixture_id": fixture_id,
        "run_id": run_id,
        "request_fingerprint": fingerprint,
        "fingerprint": fingerprint,
        "fingerprint_algorithm": "SHA-256",
        "status": "pass",
        "outcome": "pass",
        "reason": "matched",
    }],
})
write(root / "outcome.json", {
    "run_id": run_id,
    "protocol": protocol,
    "status": "pass",
    "classification": "pass",
    "release_evidence": True,
})
write(case_dir / "case.json", {
    **metadata(),
    "endpoint": {"method": "GET", "path": "/loki/api/v1/query_range"},
    "capability": "query_range",
    "expected_behavior": "HTTP 200 with streams envelope",
    "normalization_policy": "loki-v1",
    "reference_version": "grafana/loki:3.1.0",
})
write(case_dir / "case_provenance.json", {
    **metadata(),
    "canonical_request": request,
    "canonical_text": json.dumps(request, sort_keys=True, separators=(",", ":")),
    "request_fingerprint": fingerprint,
    "fingerprint_algorithm": "SHA-256",
    "release_evidence": True,
})
write(case_dir / "request.raw.json", {**metadata(), "request": request})
write(case_dir / "request.normalized.json", {**metadata(), "request": request})
write(case_dir / "softprobe.raw.json", {**metadata(), "response": normalized})
write(case_dir / "softprobe.normalized.json", {**metadata(), "response": normalized})
write(case_dir / "reference.raw.json", {**metadata(), "response": normalized})
write(case_dir / "reference.normalized.json", {**metadata(), "response": normalized})
write(case_dir / "diff.json", {
    **metadata(),
    "equal": True,
    "classification": "pass",
    "release_evidence": True,
})
write(case_dir / "outcome.json", {
    **metadata(),
    "status": "pass",
    "classification": "pass",
    "release_evidence": True,
    "evidence": {
        "raw": "reference.raw.json",
        "normalized": "reference.normalized.json",
        "diff": "diff.json",
    },
})

# Keep the index self-checking in the fixture itself.  The validator must still
# verify that every listed path exists and that no evidence file is omitted.
for relative in json.loads((root / "artifact-index.json").read_text())["artifacts"]:
    assert (root / relative).is_file(), relative
PY
}

mutate_json() {
	local path=$1
	local expression=$2
	python3 - "$path" "$expression" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expression = sys.argv[2]
value = json.loads(path.read_text(encoding="utf-8"))

if expression == "missing-classification":
    value.pop("classification", None)
elif expression == "run-id":
    value["run_id"] = "different-run"
elif expression == "fingerprint":
    value["request_fingerprint"] = "b" * 64
elif expression == "credential-leak":
    value["request"]["headers"]["Authorization"] = "Bearer super-secret-token"
elif expression == "mock-release":
    value.update({"mode": "mock", "validation_only": True, "release_evidence": True})
elif expression == "validation-only-release":
    value.update({"mode": "validation", "validation_only": True, "release_evidence": True})
elif expression == "product-regression":
    value.update({"status": "product_regression", "classification": "product_regression", "release_evidence": False})
elif expression == "receipt-empty-cases":
    value["cases"] = []
elif expression == "receipt-duplicate-case":
    value["cases"].append(dict(value["cases"][0]))
elif expression == "receipt-extra-case":
    value["cases"].append({
        "case_id": "logs-999",
        "fixture_id": "logs-basic",
        "run_id": value["run_id"],
        "request_fingerprint": "e" * 64,
        "fingerprint": "e" * 64,
        "fingerprint_algorithm": "SHA-256",
        "status": "pass",
        "outcome": "pass",
        "reason": "not executed",
    })
elif expression == "evidence-null-pointer":
    value["evidence"]["raw"] = None
elif expression == "evidence-escape":
    value["evidence"]["escaped"] = "../../suite/case-logs-001/diff.json"
elif expression == "evidence-unindexed":
    value["evidence"]["notes"] = "notes.txt"
elif expression == "index-non-json-log":
    value["artifacts"].append("suite/case-logs-001/access.log")
else:
    raise SystemExit(f"unknown fixture mutation: {expression}")

path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_validator() {
	local root=$1
	shift
	"$VALIDATOR" --root "$root" "$@"
}

expect_result() {
	local label=$1
	local expected=$2
	local root=$3
	shift 3
	local output rc

	if [ ! -e "$VALIDATOR" ]; then
		fail "$label" "validator missing: $VALIDATOR"
		return
	fi

	set +e
	output=$(run_validator "$root" "$@" 2>&1)
	rc=$?
	set -e
	if [ "$rc" -eq "$expected" ]; then
		pass "$label"
	else
		fail "$label" "expected exit $expected, got $rc: $output"
	fi
}

BASE="$TMP_DIR/complete"
write_fixture "$BASE"

# A complete real run must be accepted.  All subsequent fixtures exercise
# fail-closed validation of required metadata, both sides, index integrity,
# classifications, redaction, and release-evidence semantics.
expect_result "complete real artifact set is accepted" 0 "$BASE" --release-gate

MISSING="$TMP_DIR/missing-reference-side"
cp -R "$BASE" "$MISSING"
rm "$MISSING/suite/case-logs-001/reference.normalized.json"
expect_result "missing required reference side is rejected" 1 "$MISSING" --release-gate

NULL_SIDE="$TMP_DIR/null-reference-side"
cp -R "$BASE" "$NULL_SIDE"
printf 'null\n' >"$NULL_SIDE/suite/case-logs-001/reference.raw.json"
expect_result "null required side is rejected" 1 "$NULL_SIDE" --release-gate

RUN_MISMATCH="$TMP_DIR/run-id-mismatch"
cp -R "$BASE" "$RUN_MISMATCH"
mutate_json "$RUN_MISMATCH/suite/case-logs-001/reference.raw.json" run-id
expect_result "suite case and side run IDs must agree" 1 "$RUN_MISMATCH" --release-gate

FINGERPRINT_MISMATCH="$TMP_DIR/fingerprint-mismatch"
cp -R "$BASE" "$FINGERPRINT_MISMATCH"
mutate_json "$FINGERPRINT_MISMATCH/suite/case-logs-001/reference.raw.json" fingerprint
expect_result "suite case and side fingerprints must agree" 1 "$FINGERPRINT_MISMATCH" --release-gate

CLASSIFICATION="$TMP_DIR/missing-classification"
cp -R "$BASE" "$CLASSIFICATION"
mutate_json "$CLASSIFICATION/suite/case-logs-001/outcome.json" missing-classification
expect_result "outcomes require an explicit classification" 1 "$CLASSIFICATION" --release-gate

REDACTION="$TMP_DIR/credential-leak"
cp -R "$BASE" "$REDACTION"
mutate_json "$REDACTION/suite/case-logs-001/request.raw.json" credential-leak
expect_result "credential-bearing artifacts must be redacted" 1 "$REDACTION" --release-gate

INDEX_MISSING="$TMP_DIR/index-missing"
cp -R "$BASE" "$INDEX_MISSING"
rm "$INDEX_MISSING/suite/case-logs-001/diff.json"
expect_result "artifact index completeness is enforced" 1 "$INDEX_MISSING" --release-gate

INDEX_EXTRA="$TMP_DIR/index-extra"
cp -R "$BASE" "$INDEX_EXTRA"
printf '%s\n' '{"unindexed":true}' >"$INDEX_EXTRA/suite/case-logs-001/unindexed.json"
expect_result "unindexed evidence files are rejected" 1 "$INDEX_EXTRA" --release-gate

MOCK="$TMP_DIR/mock-release"
cp -R "$BASE" "$MOCK"
mutate_json "$MOCK/outcome.json" mock-release
expect_result "mock validation never satisfies release evidence" 1 "$MOCK" --release-gate

VALIDATION_ONLY="$TMP_DIR/validation-only-release"
cp -R "$BASE" "$VALIDATION_ONLY"
mutate_json "$VALIDATION_ONLY/outcome.json" validation-only-release
expect_result "validation-only runs never satisfy release evidence" 1 "$VALIDATION_ONLY" --release-gate

CLASSIFIED_FAILURE="$TMP_DIR/classified-failure"
cp -R "$BASE" "$CLASSIFIED_FAILURE"
mutate_json "$CLASSIFIED_FAILURE/outcome.json" product-regression
expect_result "supported failure classifications remain explicit" 0 "$CLASSIFIED_FAILURE"

# Execution-receipt selected/executed case IDs must be represented exactly by
# the receipt case records: no record, duplicate records, or stray records fail.
RECEIPT_MISSING="$TMP_DIR/receipt-missing-case-record"
cp -R "$BASE" "$RECEIPT_MISSING"
mutate_json "$RECEIPT_MISSING/execution-receipt.json" receipt-empty-cases
expect_result "selected/executed case ids each need a receipt case record" 1 "$RECEIPT_MISSING" --release-gate

RECEIPT_DUP="$TMP_DIR/receipt-duplicate-case-record"
cp -R "$BASE" "$RECEIPT_DUP"
mutate_json "$RECEIPT_DUP/execution-receipt.json" receipt-duplicate-case
expect_result "duplicate receipt case records are rejected" 1 "$RECEIPT_DUP" --release-gate

RECEIPT_EXTRA="$TMP_DIR/receipt-extra-case-record"
cp -R "$BASE" "$RECEIPT_EXTRA"
mutate_json "$RECEIPT_EXTRA/execution-receipt.json" receipt-extra-case
expect_result "receipt case records must match selected/executed case ids exactly" 1 "$RECEIPT_EXTRA" --release-gate

# Outcome evidence pointers must be non-null safe relative paths that are also
# listed in the artifact index.
EVIDENCE_NULL="$TMP_DIR/evidence-null-pointer"
cp -R "$BASE" "$EVIDENCE_NULL"
mutate_json "$EVIDENCE_NULL/suite/case-logs-001/outcome.json" evidence-null-pointer
expect_result "outcome evidence pointers must be non-null" 1 "$EVIDENCE_NULL" --release-gate

EVIDENCE_ESCAPE="$TMP_DIR/evidence-escape"
cp -R "$BASE" "$EVIDENCE_ESCAPE"
mutate_json "$EVIDENCE_ESCAPE/suite/case-logs-001/outcome.json" evidence-escape
expect_result "outcome evidence pointers must stay inside the case directory" 1 "$EVIDENCE_ESCAPE" --release-gate

EVIDENCE_UNINDEXED="$TMP_DIR/evidence-unindexed"
cp -R "$BASE" "$EVIDENCE_UNINDEXED"
printf '%s\n' 'notes for case logs-001' >"$EVIDENCE_UNINDEXED/suite/case-logs-001/notes.txt"
mutate_json "$EVIDENCE_UNINDEXED/suite/case-logs-001/outcome.json" evidence-unindexed
expect_result "outcome evidence pointers must reference indexed artifacts" 1 "$EVIDENCE_UNINDEXED" --release-gate

# Index completeness and credential redaction must cover non-JSON artifacts
# (logs, text) too, not only JSON files.
INDEX_NONJSON="$TMP_DIR/unindexed-non-json-log"
cp -R "$BASE" "$INDEX_NONJSON"
printf '%s\n' '[2026-08-15T00:00:00Z] INFO request logged' >"$INDEX_NONJSON/suite/case-logs-001/console.log"
expect_result "artifact index completeness covers non-JSON log artifacts" 1 "$INDEX_NONJSON" --release-gate

REDACT_NONJSON="$TMP_DIR/non-json-credential-leak"
cp -R "$BASE" "$REDACT_NONJSON"
printf '%s\n' '[2026-08-15T00:00:00Z] INFO request Authorization: Bearer super-secret-token' >"$REDACT_NONJSON/suite/case-logs-001/access.log"
mutate_json "$REDACT_NONJSON/artifact-index.json" index-non-json-log
expect_result "credential redaction covers non-JSON log artifacts" 1 "$REDACT_NONJSON" --release-gate

printf '\n%d passed, %d failed\n' "$passed" "$failed"
if [ "$failed" -ne 0 ]; then
	exit 1
fi
