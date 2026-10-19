"""Guard the production vs offline-benchmark package boundary."""

from __future__ import annotations

import ast
from pathlib import Path

import lisa.evaluation

_LISA_ROOT = Path(__file__).resolve().parents[1]
_RUNTIME_SRC = _LISA_ROOT / "src" / "lisa"
_BENCHMARKS_ROOT = _LISA_ROOT / "benchmarks"

# These modules used to live under lisa.evaluation and must not return there.
_OFFLINE_HARNESS_MODULES = frozenset(
    {
        "activity_dataset.py",
        "activity_benchmark.py",
        "dataset.py",
        "dataset_benchmark.py",
        "policy_dataset_ingest.py",
    }
)


def _imports_lisa_benchmarks(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "lisa_benchmarks" or alias.name.startswith("lisa_benchmarks.") for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "lisa_benchmarks" or module.startswith("lisa_benchmarks."):
                return True
    return False


def test_runtime_evaluation_package_excludes_offline_harness_modules():
    evaluation_dir = Path(lisa.evaluation.__file__).resolve().parent
    present = {path.name for path in evaluation_dir.glob("*.py")}
    leaked = sorted(_OFFLINE_HARNESS_MODULES & present)
    assert not leaked, (
        "Offline benchmark harness must not live under lisa.evaluation; "
        f"found {leaked}. Keep them in lisa/benchmarks/lisa_benchmarks/."
    )


def test_runtime_package_does_not_import_offline_harness():
    offenders = [
        str(path.relative_to(_LISA_ROOT))
        for path in _RUNTIME_SRC.rglob("*.py")
        if _imports_lisa_benchmarks(path)
    ]
    assert not offenders, (
        "Runtime lisa.* must never import lisa_benchmarks; "
        f"found imports in {offenders}"
    )


def test_offline_harness_package_is_importable_outside_runtime_tree():
    import lisa_benchmarks
    import lisa_benchmarks.activity_dataset
    import lisa_benchmarks.dataset

    harness_file = Path(lisa_benchmarks.__file__).resolve()
    runtime_eval = Path(lisa.evaluation.__file__).resolve()
    benchmarks_root = harness_file.parents[1]
    assert benchmarks_root == _BENCHMARKS_ROOT.resolve()
    assert runtime_eval.is_relative_to(_RUNTIME_SRC.resolve())
    assert Path(lisa_benchmarks.activity_dataset.__file__).is_relative_to(benchmarks_root)
    assert Path(lisa_benchmarks.dataset.__file__).is_relative_to(benchmarks_root)
