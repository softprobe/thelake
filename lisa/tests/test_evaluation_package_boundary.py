"""Layout guards: main / tests / datasets(+loaders) / benchmark(runners)."""

from __future__ import annotations

import ast
from pathlib import Path

import lisa.evaluation


_LISA_ROOT = Path(__file__).resolve().parents[1]
_RUNTIME_SRC = _LISA_ROOT / "src" / "lisa"
_BENCHMARK_ROOT = _LISA_ROOT / "benchmark"
_DATASETS_ROOT = _LISA_ROOT / "datasets"

_FORBIDDEN_IN_EVALUATION = frozenset(
    {
        "activity_dataset.py",
        "activity_runner.py",
        "policy_dataset.py",
        "policy_runner.py",
        "policy_ingest.py",
    }
)


def _imports_name(path: Path, root: str) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == root or alias.name.startswith(root + ".") for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == root or module.startswith(root + "."):
                return True
    return False


def test_runtime_evaluation_excludes_benchmark_and_dataset_modules():
    evaluation_dir = Path(lisa.evaluation.__file__).resolve().parent
    present = {path.name for path in evaluation_dir.glob("*.py")}
    leaked = sorted(_FORBIDDEN_IN_EVALUATION & present)
    assert not leaked, f"Keep loaders in datasets/ and runners in benchmark/: {leaked}"


def test_runtime_package_does_not_import_benchmark_or_datasets():
    offenders = [
        str(path.relative_to(_LISA_ROOT))
        for path in _RUNTIME_SRC.rglob("*.py")
        if _imports_name(path, "benchmark") or _imports_name(path, "datasets")
    ]
    assert not offenders, f"Runtime lisa.* must not import benchmark/datasets: {offenders}"


def test_benchmark_has_runners_only():
    assert (_BENCHMARK_ROOT / "activity_runner.py").is_file()
    assert (_BENCHMARK_ROOT / "policy_runner.py").is_file()
    assert not (_BENCHMARK_ROOT / "activity_dataset.py").exists()
    assert not (_BENCHMARK_ROOT / "policy_dataset.py").exists()
    assert not (_BENCHMARK_ROOT / "tests").exists()
    assert not (_BENCHMARK_ROOT / "datasets").exists()


def test_datasets_package_owns_loaders_and_resources():
    import datasets.activity_dataset
    import datasets.policy_dataset

    root = Path(datasets.activity_dataset.__file__).resolve().parent
    assert root == _DATASETS_ROOT.resolve()
    assert (_DATASETS_ROOT / "activity_management").is_dir()
    assert (_DATASETS_ROOT / "policy_evaluation").is_dir()
    assert Path(datasets.policy_dataset.__file__).resolve().parent == root
