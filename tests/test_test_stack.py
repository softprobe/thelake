"""Contract tests for the shared automated-test Docker lifecycle."""

import json
import importlib.util
import subprocess
import tempfile
from argparse import Namespace
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "test_stack.py"
SPEC = importlib.util.spec_from_file_location("thelake_test_stack", SCRIPT)
test_stack = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(test_stack)


def test_compose_teardown_captures_output_keeps_artifacts_and_removes_scratch(
    tmp_path, monkeypatch
):
    run_id = "123456789abc"
    artifact_dir = tmp_path / "test-artifacts"
    artifact_dir.mkdir()
    scratch_dir = Path(tempfile.mkdtemp(prefix=f"thelake-test-{run_id}-"))
    (scratch_dir / "lake-data").mkdir()
    report = artifact_dir / "report.json"
    report.write_text("{}\n", encoding="utf-8")
    state_file = artifact_dir / "stack.json"
    state_file.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "suite": "sample",
                "project_name": f"thelake-test-sample-{run_id}",
                "artifact_dir": str(artifact_dir),
                "scratch_dir": str(scratch_dir),
                "state_file": str(state_file),
                "kind": "compose",
                "compose_files": [str(tmp_path / "compose.yaml")],
                "env_files": [],
                "services": ["postgres"],
                "status": "running",
            }
        ),
        encoding="utf-8",
    )
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        if "ps" in command:
            return subprocess.CompletedProcess(command, 0, "service running\n", "")
        if "logs" in command:
            return subprocess.CompletedProcess(command, 0, "test service log\n", "")
        if "down" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)

    state = test_stack._stop(state_file)

    assert commands[-1][-3:] == ["down", "--volumes", "--remove-orphans"]
    assert (artifact_dir / "compose-ps.txt").read_text() == "service running\n"
    assert (artifact_dir / "compose-logs.txt").read_text() == "test service log\n"
    assert report.exists()
    assert not scratch_dir.exists()
    assert state["status"] == "stopped"


def test_container_start_routes_through_the_shared_owner_and_reports_ports(
    tmp_path, monkeypatch
):
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        if command[:2] == ["docker", "run"]:
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        if command[:2] == ["docker", "port"]:
            return subprocess.CompletedProcess(command, 0, "127.0.0.1:43123\n", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="compat-loki",
        artifact_dir=tmp_path / "artifacts",
        state_file=None,
        image="grafana/loki:test",
        docker_option=["-p", "127.0.0.1::3100"],
        published_port=["3100"],
        container_command=["--", "-config.file=/etc/loki.yaml"],
    )

    state = test_stack._start_container(args)

    assert state["ports"] == {"3100": ["127.0.0.1:43123"]}
    assert state["container_id"] == "container-id"
    assert commands[0][-2:] == ["grafana/loki:test", "-config.file=/etc/loki.yaml"]
    assert f"{test_stack.RUN_LABEL}={state['run_id']}" in commands[0]
