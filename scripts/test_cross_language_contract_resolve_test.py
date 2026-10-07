"""Unit tests for cross-language contract gate resolution helpers."""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "test_cross_language_contract.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "test_cross_language_contract",
        MODULE_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module()


def test_resolve_prefers_softprobe_js_root_env(tmp_path: pathlib.Path) -> None:
    js = tmp_path / "custom-js"
    (js / "packages" / "tracing").mkdir(parents=True)
    resolved = mod.resolve_softprobe_js_root(
        env={"SOFTPROBE_JS_ROOT": str(js)},
        sibling=tmp_path / "missing-sibling",
        cache_dir=tmp_path / "cache",
    )
    assert resolved == js.resolve()


def test_resolve_uses_sibling_when_env_unset(tmp_path: pathlib.Path) -> None:
    sibling = tmp_path / "softprobe-js"
    (sibling / "packages" / "tracing").mkdir(parents=True)
    resolved = mod.resolve_softprobe_js_root(
        env={},
        sibling=sibling,
        cache_dir=tmp_path / "cache",
    )
    assert resolved == sibling.resolve()


def test_resolve_rejects_checkout_without_tracing(tmp_path: pathlib.Path) -> None:
    empty = tmp_path / "empty-js"
    empty.mkdir()
    with pytest.raises(SystemExit, match="packages/tracing"):
        mod.resolve_softprobe_js_root(
            env={"SOFTPROBE_JS_ROOT": str(empty)},
            sibling=tmp_path / "missing",
            cache_dir=tmp_path / "cache",
        )
