#!/usr/bin/env python3
"""Fast in-memory cross-language contract gate for CI.

Runs shared fixture validation, then both SDK suites against this repository's
authoritative ``contracts/fixtures``:

- TypeScript: ``@softprobe/tracing`` tests in softprobe/softprobe-js
- Python: sibling ``softprobe-py/tests``

The TypeScript checkout is resolved from ``SOFTPROBE_JS_ROOT``, else the sibling
``../softprobe-js`` directory, else a shallow clone under ``.cache/softprobe-js``.
The Python checkout is resolved from ``SOFTPROBE_PY_ROOT`` or the sibling
``../softprobe-py`` directory; unlike the JS checkout, it is never cloned by CI.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "contracts" / "fixtures"
DEFAULT_SOFTPROBE_JS_URL = "https://github.com/softprobe/softprobe-js.git"
CACHE_SOFTPROBE_JS = ROOT / ".cache" / "softprobe-js"


def resolve_softprobe_js_root(
    *,
    env: dict[str, str] | None = None,
    sibling: pathlib.Path | None = None,
    cache_dir: pathlib.Path | None = None,
) -> pathlib.Path:
    """Locate a softprobe-js checkout that contains packages/tracing."""
    environ = env if env is not None else os.environ
    explicit = environ.get("SOFTPROBE_JS_ROOT", "").strip()
    if explicit:
        root = pathlib.Path(explicit).expanduser().resolve()
        _require_tracing_package(root, source=f"SOFTPROBE_JS_ROOT={root}")
        return root

    sibling_root = (
        sibling
        if sibling is not None
        else ROOT.parent / "softprobe-js"
    ).resolve()
    if sibling_root.is_dir():
        _require_tracing_package(sibling_root, source=f"sibling {sibling_root}")
        return sibling_root

    cache_root = (cache_dir if cache_dir is not None else CACHE_SOFTPROBE_JS).resolve()
    _ensure_cached_clone(cache_root, environ.get("SOFTPROBE_JS_GIT_URL", DEFAULT_SOFTPROBE_JS_URL))
    _require_tracing_package(cache_root, source=f"cache {cache_root}")
    return cache_root


def _require_tracing_package(root: pathlib.Path, *, source: str) -> None:
    tracing = root / "packages" / "tracing"
    if not tracing.is_dir():
        raise SystemExit(
            f"softprobe-js checkout is missing packages/tracing ({source}). "
            "Set SOFTPROBE_JS_ROOT or clone softprobe/softprobe-js as a sibling."
        )


def _ensure_cached_clone(cache_root: pathlib.Path, git_url: str) -> None:
    cache_root.parent.mkdir(parents=True, exist_ok=True)
    if (cache_root / ".git").is_dir():
        subprocess.run(
            ["git", "-C", str(cache_root), "fetch", "--depth", "1", "origin", "main"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(cache_root), "reset", "--hard", "origin/main"],
            check=True,
        )
        return
    if cache_root.exists():
        shutil.rmtree(cache_root)
    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", "main", git_url, str(cache_root)],
        check=True,
    )


def run_typescript_contract_suite(js_root: pathlib.Path) -> None:
    """Run softprobe-js tracing tests against this repo's contracts/fixtures."""
    env = {
        **os.environ,
        "SOFTPROBE_CONTRACTS_ROOT": str(ROOT / "contracts"),
    }
    subprocess.run(
        ["npm", "ci"],
        check=True,
        cwd=js_root,
        env=env,
    )
    subprocess.run(
        ["npm", "run", "test", "--workspace", "@softprobe/tracing"],
        check=True,
        cwd=js_root,
        env=env,
    )


def run_python_contract_suite() -> None:
    explicit = os.environ.get("SOFTPROBE_PY_ROOT", "").strip()
    python_root = pathlib.Path(explicit).expanduser().resolve() if explicit else ROOT.parent / "softprobe-py"
    if not (python_root / "pyproject.toml").is_file():
        raise SystemExit(
            f"softprobe-py checkout not found at {python_root}; set SOFTPROBE_PY_ROOT"
        )
    env = {**os.environ, "SOFTPROBE_CONTRACTS_ROOT": str(ROOT / "contracts")}
    subprocess.run(
        ["uv", "run", "--locked", "--project", str(python_root), "--extra", "dev", "pytest", "tests", "-q"],
        check=True,
        cwd=python_root,
        env=env,
    )


def main() -> None:
    subprocess.run([sys.executable, str(ROOT / "scripts" / "validate_contracts.py")], check=True)

    expected_spans = json.loads((FIXTURES / "expected-nested-spans.json").read_text())
    expected_scores = json.loads((FIXTURES / "expected-scores.json").read_text())
    types = {span["observation_type"] for span in expected_spans}
    assert "generation" in types and "agent" in types
    assert len(expected_scores) == 3

    js_root = resolve_softprobe_js_root()
    print(f"Running TypeScript contract suite from {js_root} against {FIXTURES}")
    run_typescript_contract_suite(js_root)
    run_python_contract_suite()
    print("Cross-language in-memory contract checks passed.")


if __name__ == "__main__":
    main()
