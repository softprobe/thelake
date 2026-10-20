"""Unit coverage for Lisa disposable Postgres lifecycle ownership."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import conftest as lisa_env


@pytest.fixture(autouse=True)
def clear_active_stacks(monkeypatch):
    with lisa_env._ACTIVE_POSTGRES_LOCK:
        lisa_env._ACTIVE_POSTGRES.clear()
    monkeypatch.setattr(lisa_env, "_EXIT_HOOKS_INSTALLED", False)
    yield
    with lisa_env._ACTIVE_POSTGRES_LOCK:
        lisa_env._ACTIVE_POSTGRES.clear()
    monkeypatch.setattr(lisa_env, "_EXIT_HOOKS_INSTALLED", False)


def test_start_registers_stack_and_stop_untracks(monkeypatch, tmp_path):
    state_file = tmp_path / "stack.json"
    state_file.write_text("{}", encoding="utf-8")
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if "start-compose" in command:
            payload = {
                "state_file": str(state_file),
                "ports": {"lisa-postgres:5432": ["127.0.0.1:65432"]},
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        if "stop" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(command)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(lisa_env.subprocess, "run", run)
    monkeypatch.setattr(lisa_env.psycopg, "connect", lambda *_a, **_k: FakeConnection())
    registered = []
    monkeypatch.setattr(lisa_env.atexit, "register", registered.append)

    stack = lisa_env.start_lisa_postgres(suite="lisa-lifecycle-unit")
    assert str(stack.state_file) in lisa_env._ACTIVE_POSTGRES
    assert registered == [lisa_env.stop_all_lisa_postgres]

    lisa_env.stop_lisa_postgres(stack)
    assert lisa_env._ACTIVE_POSTGRES == {}
    assert any("stop" in part for command in calls for part in command)


def test_failed_connect_stops_started_stack(monkeypatch, tmp_path):
    state_file = tmp_path / "stack.json"
    state_file.write_text("{}", encoding="utf-8")
    stops = []

    def run(command, **kwargs):
        if "start-compose" in command:
            payload = {
                "state_file": str(state_file),
                "ports": {"lisa-postgres:5432": ["127.0.0.1:65432"]},
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        if "stop" in command:
            stops.append(command)
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(command)

    monkeypatch.setattr(lisa_env.subprocess, "run", run)
    monkeypatch.setattr(lisa_env.atexit, "register", lambda *_a, **_k: None)

    def boom(*_a, **_k):
        raise RuntimeError("refused")

    monkeypatch.setattr(lisa_env.psycopg, "connect", boom)

    with pytest.raises(RuntimeError, match="refused"):
        lisa_env.start_lisa_postgres(suite="lisa-lifecycle-unit")
    assert stops
    assert lisa_env._ACTIVE_POSTGRES == {}


def test_stop_all_drains_every_tracked_stack(monkeypatch, tmp_path):
    first = lisa_env.LisaPostgresStack("postgresql://a", tmp_path / "a.json")
    second = lisa_env.LisaPostgresStack("postgresql://b", tmp_path / "b.json")
    lisa_env._track_postgres_stack(first)
    lisa_env._track_postgres_stack(second)
    stopped = []

    def fake_stop(stack):
        stopped.append(stack.state_file.name)
        lisa_env._untrack_postgres_stack(stack)

    monkeypatch.setattr(lisa_env, "stop_lisa_postgres", fake_stop)
    lisa_env.stop_all_lisa_postgres()
    assert sorted(stopped) == ["a.json", "b.json"]
    assert lisa_env._ACTIVE_POSTGRES == {}


def test_failed_stop_keeps_stack_tracked_for_retry(monkeypatch, tmp_path):
    stack = lisa_env.LisaPostgresStack("postgresql://a", tmp_path / "a.json")
    lisa_env._track_postgres_stack(stack)

    def boom(*_a, **_k):
        raise subprocess.CalledProcessError(1, ["stop"])

    monkeypatch.setattr(lisa_env.subprocess, "run", boom)
    with pytest.raises(subprocess.CalledProcessError):
        lisa_env.stop_lisa_postgres(stack)
    assert str(stack.state_file) in lisa_env._ACTIVE_POSTGRES


def test_stop_all_retries_after_failed_stop_and_surfaces_error(monkeypatch, tmp_path):
    stack = lisa_env.LisaPostgresStack("postgresql://a", tmp_path / "a.json")
    lisa_env._track_postgres_stack(stack)
    attempts = {"count": 0}

    def flaky_stop(target):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("docker unavailable")
        lisa_env._untrack_postgres_stack(target)

    monkeypatch.setattr(lisa_env, "stop_lisa_postgres", flaky_stop)
    with pytest.raises(RuntimeError, match="docker unavailable"):
        lisa_env.stop_all_lisa_postgres()
    assert str(stack.state_file) in lisa_env._ACTIVE_POSTGRES
    lisa_env.stop_all_lisa_postgres()
    assert lisa_env._ACTIVE_POSTGRES == {}
    assert attempts["count"] == 2
