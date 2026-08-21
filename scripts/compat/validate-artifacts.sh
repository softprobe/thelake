#!/usr/bin/env bash

set -euo pipefail

# Validates a conformance artifact set for completeness, consistency, and
# release-worthiness.  Fails closed: any missing required artifact, metadata
# mismatch, unknown or missing classification, credential leak, index
# inconsistency, or null/missing side rejects the set.  Mock and
# validation-only artifacts may validate as non-release but never satisfy
# release evidence.

usage() {
	cat >&2 <<'EOF'
Usage: validate-artifacts.sh --root DIR [--release-gate]

  --root DIR        artifact root containing artifact-index.json,
                    execution-receipt.json, and outcome.json
  --release-gate    require the set to certify a release; mock, validation,
                    and drift artifacts never satisfy release evidence
EOF
	exit 2
}

ROOT=
GATE=0

while [ "$#" -gt 0 ]; do
	case "$1" in
		--root)
			[ "$#" -ge 2 ] || usage
			ROOT=$2
			shift 2
			;;
		--release-gate)
			GATE=1
			shift
			;;
		*)
			usage
			;;
	esac
done

if [ -z "$ROOT" ]; then
	echo "validate-artifacts: ERROR: --root DIR is required" >&2
	usage
fi

if [ ! -d "$ROOT" ]; then
	echo "validate-artifacts: ERROR: artifact root is not a directory: $ROOT" >&2
	exit 2
fi

python3 - "$ROOT" "$GATE" <<'PY'
import json
import os
import re
import sys

root = sys.argv[1]
release_gate = sys.argv[2] == "1"

errors = []


def error(path, message):
    errors.append("%s: %s" % (path, message))


KNOWN_CLASSIFICATIONS = frozenset(
    [
        "pass",
        "drift",
        "product_regression",
        "infrastructure_failure",
        "environment_skip",
        "unsupported",
        "skipped",
        "mismatched_provenance",
        "receipt_validation_failure",
        "invalid_execution_receipt",
    ]
)

PLACEHOLDER = re.compile(r"^\[?REDACTED\]?$", re.IGNORECASE)
BEARER = re.compile(r"\bBearer\s+([^\s,}]+)", re.IGNORECASE)
KV_SECRET = re.compile(
    r"\b(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|api[_-]?key|apikey|client[_-]?secret)[=:]\s*(\S+)",
    re.IGNORECASE,
)
SECRET_KEY = re.compile(
    r"(?i)\b(?:authorization|proxy[_-]?authorization|password|passwd|secret|token|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret)\b"
)

REQUIRED_CASE_FILES = [
    "case.json",
    "case_provenance.json",
    "request.raw.json",
    "request.normalized.json",
    "softprobe.raw.json",
    "softprobe.normalized.json",
    "reference.raw.json",
    "reference.normalized.json",
    "diff.json",
    "outcome.json",
]

CASE_META = ("run_id", "case_id", "protocol", "request_fingerprint")


def all_json_files():
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in sorted(filenames):
            if name.endswith(".json"):
                yield os.path.normpath(os.path.relpath(os.path.join(dirpath, name), root))


def all_regular_files():
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in sorted(filenames):
            rel = os.path.normpath(os.path.relpath(os.path.join(dirpath, name), root))
            if rel and os.path.isfile(os.path.join(root, rel)):
                yield rel


def load_json(rel):
    full = os.path.join(root, rel)
    try:
        with open(full, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        error(rel, "missing required artifact")
        return None
    except json.JSONDecodeError as exc:
        error(rel, "invalid JSON: %s" % exc)
        return None


def is_redacted(value):
    value = value.strip()
    if re.match(r"(?i)^Bearer\s+\[?REDACTED\]?$", value):
        return True
    return bool(PLACEHOLDER.match(value))


def scan_string(value, path, where):
    for match in BEARER.finditer(value):
        token = match.group(1).strip("\"'[,]")
        if not PLACEHOLDER.match(token):
            error(path, "credential leak: %s embeds a Bearer token that is not redacted" % where)
    for match in KV_SECRET.finditer(value):
        token = match.group(1).strip("\"'[,]")
        if not PLACEHOLDER.match(token):
            error(path, "credential leak: %s embeds a secret value that is not redacted: %s" % (where, match.group(0)))


def scan_value(value, path, where, depth=0):
    if depth > 64:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, str):
                if SECRET_KEY.search(key):
                    if not is_redacted(item):
                        error(path, "credential leak: %s.%s=%r is not redacted" % (where, key, item))
                else:
                    scan_string(item, path, "%s.%s" % (where, key))
            else:
                scan_value(item, path, "%s.%s" % (where, key), depth + 1)
    elif isinstance(value, list):
        for position, item in enumerate(value):
            scan_value(item, path, "%s[%d]" % (where, position), depth + 1)


def require_string(obj, path, key):
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        error(path, "field '%s' must be a non-empty string" % key)
        return None
    return value


def check_classification(obj, path, label):
    classification = obj.get("classification")
    if not isinstance(classification, str) or not classification:
        error(path, "%s is missing an explicit classification" % label)
        return
    if classification not in KNOWN_CLASSIFICATIONS:
        error(path, "unknown classification %r" % classification)


def load_case_file(cdir, name):
    rel = os.path.join(cdir, name)
    full = os.path.join(root, rel)
    try:
        with open(full, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except FileNotFoundError:
        error(rel, "missing required %s artifact" % name)
        return None
    except json.JSONDecodeError as exc:
        error(rel, "invalid JSON: %s" % exc)
        return None
    if not isinstance(value, dict):
        error(rel, "must be a non-null JSON object")
        return None
    return value


# --- artifact index ---------------------------------------------------------
index = load_json("artifact-index.json")
index_ok = isinstance(index, dict)
if index is not None and not index_ok:
    error("artifact-index.json", "must be a JSON object")

suite_run_id = None
suite_protocol = None
indexed = set()

if index_ok:
    suite_run_id = index.get("run_id")
    suite_protocol = index.get("protocol")
    if not isinstance(suite_run_id, str) or not suite_run_id:
        error("artifact-index.json", "run_id must be a non-empty string")
        suite_run_id = None
    if not isinstance(suite_protocol, str) or not suite_protocol:
        error("artifact-index.json", "protocol must be a non-empty string")
        suite_protocol = None
    if not isinstance(index.get("schema_version"), str) or not index["schema_version"]:
        error("artifact-index.json", "schema_version must be a non-empty string")
    if not isinstance(index.get("release_evidence"), bool):
        error("artifact-index.json", "release_evidence must be a boolean")
    artifacts = index.get("artifacts")
    if not isinstance(artifacts, list):
        error("artifact-index.json", "artifacts must be an array of relative paths")
        artifacts = []
    for entry in artifacts:
        if not isinstance(entry, str) or not entry:
            error("artifact-index.json", "artifacts entries must be non-empty relative paths")
            continue
        rel = os.path.normpath(entry)
        if os.path.isabs(entry) or rel == "." or rel.startswith(".."):
            error("artifact-index.json", "artifacts entry escapes the artifact root: %r" % entry)
            continue
        full = os.path.join(root, rel)
        if not os.path.isfile(full):
            error("artifact-index.json", "indexed artifact is missing: %s" % rel)
            continue
        indexed.add(rel)

# --- execution receipt ------------------------------------------------------
receipt = load_json("execution-receipt.json")
if isinstance(receipt, dict):
    if suite_run_id is not None and receipt.get("run_id") != suite_run_id:
        error("execution-receipt.json", "run_id %r does not match suite run_id %r" % (receipt.get("run_id"), suite_run_id))
    if suite_protocol is not None and receipt.get("protocol") != suite_protocol:
        error("execution-receipt.json", "protocol %r does not match suite protocol %r" % (receipt.get("protocol"), suite_protocol))
    if not isinstance(receipt.get("status"), str) or not receipt["status"]:
        error("execution-receipt.json", "status must be a non-empty string")
    for key in ("selected_case_ids", "executed_case_ids", "selected_fixture_ids"):
        if key in receipt and not isinstance(receipt[key], list):
            error("execution-receipt.json", "%s must be an array" % key)
    receipt_cases = receipt.get("cases")
    if not isinstance(receipt_cases, list):
        error("execution-receipt.json", "cases must be an array of per-case records")
        receipt_cases = []
else:
    if receipt is not None:
        error("execution-receipt.json", "must be a JSON object")
    receipt_cases = []

# --- suite outcome ----------------------------------------------------------
root_outcome = load_json("outcome.json")
root_outcome_ok = isinstance(root_outcome, dict)
if root_outcome is not None and not root_outcome_ok:
    error("outcome.json", "must be a JSON object")
if root_outcome_ok:
    if suite_run_id is not None and root_outcome.get("run_id") != suite_run_id:
        error("outcome.json", "run_id %r does not match suite run_id %r" % (root_outcome.get("run_id"), suite_run_id))
    if suite_protocol is not None and root_outcome.get("protocol") != suite_protocol:
        error("outcome.json", "protocol %r does not match suite protocol %r" % (root_outcome.get("protocol"), suite_protocol))
    if not isinstance(root_outcome.get("status"), str) or not root_outcome["status"]:
        error("outcome.json", "status must be a non-empty string")
    check_classification(root_outcome, "outcome.json", "suite outcome")
    if "release_evidence" in root_outcome and not isinstance(root_outcome["release_evidence"], bool):
        error("outcome.json", "release_evidence must be a boolean")

# --- case directories -------------------------------------------------------
case_dirs = set()
for rel in indexed:
    parent = os.path.dirname(rel)
    base = os.path.basename(rel)
    if parent and base in REQUIRED_CASE_FILES:
        case_dirs.add(parent)

case_meta = {}
case_outcomes = {}


def validate_case(cdir, suite_run_id, suite_protocol):
    rels = {name: os.path.join(cdir, name) for name in REQUIRED_CASE_FILES}
    values = {name: load_case_file(cdir, name) for name in REQUIRED_CASE_FILES}

    case = values["case.json"]
    if not isinstance(case, dict):
        return

    run_id = require_string(case, rels["case.json"], "run_id")
    case_id = require_string(case, rels["case.json"], "case_id")
    protocol = require_string(case, rels["case.json"], "protocol")
    fixture_id = require_string(case, rels["case.json"], "fixture_id")
    fingerprint = require_string(case, rels["case.json"], "request_fingerprint")
    if run_id is None or case_id is None or protocol is None or fixture_id is None or fingerprint is None:
        return

    if suite_run_id is not None and run_id != suite_run_id:
        error(rels["case.json"], "run_id %r does not match suite run_id %r" % (run_id, suite_run_id))
    if suite_protocol is not None and protocol != suite_protocol:
        error(rels["case.json"], "protocol %r does not match suite protocol %r" % (protocol, suite_protocol))

    expected_meta = {
        "run_id": run_id,
        "case_id": case_id,
        "protocol": protocol,
        "request_fingerprint": fingerprint,
    }

    def check_meta(value, rel, label):
        for key in CASE_META:
            if value.get(key) != expected_meta[key]:
                error(rel, "%s %r does not match case %r" % (key, value.get(key), expected_meta[key]))

    provenance = values["case_provenance.json"]
    if isinstance(provenance, dict):
        check_meta(provenance, rels["case_provenance.json"], "case provenance")
        algorithm = provenance.get("fingerprint_algorithm")
        if not isinstance(algorithm, str) or not algorithm:
            error(rels["case_provenance.json"], "fingerprint_algorithm must be a non-empty string")

    for name in (
        "request.raw.json",
        "request.normalized.json",
        "softprobe.raw.json",
        "softprobe.normalized.json",
        "reference.raw.json",
        "reference.normalized.json",
    ):
        value = values[name]
        if isinstance(value, dict):
            check_meta(value, rels[name], name)

    diff = values["diff.json"]
    if isinstance(diff, dict):
        check_meta(diff, rels["diff.json"], "diff")
        check_classification(diff, rels["diff.json"], "diff")
        if "release_evidence" in diff and not isinstance(diff["release_evidence"], bool):
            error(rels["diff.json"], "release_evidence must be a boolean")

    outcome = values["outcome.json"]
    if isinstance(outcome, dict):
        case_outcomes[cdir] = outcome
        check_meta(outcome, rels["outcome.json"], "case outcome")
        if not isinstance(outcome.get("status"), str) or not outcome["status"]:
            error(rels["outcome.json"], "status must be a non-empty string")
        check_classification(outcome, rels["outcome.json"], "case outcome")
        if "release_evidence" in outcome and not isinstance(outcome["release_evidence"], bool):
            error(rels["outcome.json"], "release_evidence must be a boolean")
        evidence = outcome.get("evidence")
        if isinstance(evidence, dict):
            for reference in evidence.values():
                if not isinstance(reference, str) or not reference.strip():
                    error(rels["outcome.json"], "evidence pointer must be a non-null non-empty relative path")
                    continue
                if os.path.isabs(reference) or reference.startswith(os.sep):
                    error(rels["outcome.json"], "evidence pointer must stay inside the case directory: %s" % reference)
                    continue
                norm = os.path.normpath(reference)
                if norm == "." or norm.startswith(".."):
                    error(rels["outcome.json"], "evidence pointer must stay inside the case directory: %s" % reference)
                    continue
                target = os.path.normpath(os.path.join(cdir, norm))
                if not os.path.isfile(os.path.join(root, target)):
                    error(rels["outcome.json"], "evidence references missing artifact: %s" % reference)
                    continue
                if target not in indexed:
                    error(rels["outcome.json"], "evidence references unindexed artifact: %s" % reference)

    case_meta[case_id] = {"fixture_id": fixture_id, "request_fingerprint": fingerprint}


for cdir in sorted(case_dirs):
    validate_case(cdir, suite_run_id, suite_protocol)

# --- receipt per-case records ------------------------------------------------
recorded_case_ids = set()
for record_index, record in enumerate(receipt_cases):
    if not isinstance(record, dict):
        error("execution-receipt.json", "cases[%d] must be an object" % record_index)
        continue
    if suite_run_id is not None and record.get("run_id") != suite_run_id:
        error("execution-receipt.json", "cases[%d].run_id %r does not match suite run_id" % (record_index, record.get("run_id")))
    for key in ("case_id", "fixture_id", "request_fingerprint", "fingerprint", "fingerprint_algorithm"):
        if not isinstance(record.get(key), str) or not record[key]:
            error("execution-receipt.json", "cases[%d].%s must be a non-empty string" % (record_index, key))
    case_id = record.get("case_id")
    if not isinstance(case_id, str) or not case_id:
        continue
    if case_id in recorded_case_ids:
        error("execution-receipt.json", "cases[%d] duplicates the case record for %r" % (record_index, case_id))
        continue
    recorded_case_ids.add(case_id)
    meta = case_meta.get(case_id)
    if meta is not None:
        if record.get("fixture_id") != meta["fixture_id"]:
            error("execution-receipt.json", "cases[%d].fixture_id %r does not match case fixture_id %r" % (record_index, record.get("fixture_id"), meta["fixture_id"]))
        for key in ("request_fingerprint", "fingerprint"):
            if record.get(key) != meta["request_fingerprint"]:
                error("execution-receipt.json", "cases[%d].%s %r does not match case fingerprint" % (record_index, key, record.get(key)))

if isinstance(receipt, dict):
    selected_executed_ids = set()
    for key in ("selected_case_ids", "executed_case_ids"):
        entries = receipt.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, str) or not entry:
                error("execution-receipt.json", "%s entries must be non-empty strings" % key)
            else:
                selected_executed_ids.add(entry)
    for case_id in sorted(selected_executed_ids):
        if case_id not in recorded_case_ids:
            error("execution-receipt.json", "no receipt case record for selected/executed case %r" % case_id)
    for case_id in sorted(recorded_case_ids):
        if case_id not in selected_executed_ids:
            error("execution-receipt.json", "receipt case record %r does not match any selected/executed case id" % case_id)

# --- index completeness -------------------------------------------------------
for rel in all_regular_files():
    if rel == "artifact-index.json":
        continue
    if rel not in indexed:
        error(rel, "evidence file is not listed in artifact-index.json")

# --- credential redaction ------------------------------------------------------
for rel in all_regular_files():
    if rel.endswith(".json"):
        value = load_json(rel)
        if isinstance(value, dict) or isinstance(value, list):
            scan_value(value, rel, rel.replace("/", "."))
        continue
    full = os.path.join(root, rel)
    try:
        with open(full, "r", encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    except OSError as exc:
        error(rel, "cannot read artifact for credential scan: %s" % exc)
        continue
    scan_string(text, rel, rel)

# --- release gate ----------------------------------------------------------------
if release_gate:
    if index_ok and index.get("release_evidence") is not True:
        error("artifact-index.json", "release gate: index does not assert release_evidence")
    if root_outcome_ok:
        if root_outcome.get("release_evidence") is not True:
            error("outcome.json", "release gate: suite outcome does not assert release_evidence")
        if root_outcome.get("status") != "pass":
            error("outcome.json", "release gate: suite status %r is not 'pass'" % root_outcome.get("status"))
        if root_outcome.get("classification") != "pass":
            error("outcome.json", "release gate: suite classification %r is not 'pass'" % root_outcome.get("classification"))
    for rel in all_json_files():
        value = load_json(rel)
        if not isinstance(value, dict):
            continue
        mode = value.get("mode")
        if mode in ("mock", "validation", "drift"):
            error(rel, "release gate: mode %r can never satisfy release evidence" % mode)
        if value.get("validation_only") is True:
            error(rel, "release gate: validation-only artifacts can never satisfy release evidence")
    for cdir in sorted(case_dirs):
        outcome = case_outcomes.get(cdir)
        if outcome is not None:
            if outcome.get("release_evidence") is not True:
                error(os.path.join(cdir, "outcome.json"), "release gate: case outcome does not assert release_evidence")
            if outcome.get("status") != "pass":
                error(os.path.join(cdir, "outcome.json"), "release gate: case status %r is not 'pass'" % outcome.get("status"))

for message in errors:
    print("validate-artifacts: %s" % message, file=sys.stderr)
if errors:
    print("validate-artifacts: FAILED with %d error(s)" % len(errors), file=sys.stderr)
    raise SystemExit(1)
PY

exit 0
