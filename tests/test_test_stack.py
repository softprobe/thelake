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
                "compose_resources": {
                    "containers": ["container-id"],
                    "volumes": [],
                    "networks": [],
                },
            }
        ),
        encoding="utf-8",
    )
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        if command[:3] == ["docker", "ps", "--all"]:
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        if command[:3] in (
            ["docker", "volume", "ls"],
            ["docker", "network", "ls"],
        ):
            return subprocess.CompletedProcess(command, 0, "", "")
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
    test_stack._remove_scratch(state)


def test_container_start_interrupt_recovers_created_container_by_owned_name(
    tmp_path, monkeypatch
):
    artifact_dir = tmp_path / "artifacts"
    commands = []
    run_id = None

    def run(command, *, check=True):
        nonlocal run_id
        commands.append(command)
        if command[:2] == ["docker", "run"]:
            run_id = next(value.split("=", 1)[1] for value in command if value.startswith(f"{test_stack.RUN_LABEL}="))
            raise KeyboardInterrupt
        if command[:3] == ["docker", "inspect", "--format"] and command[-1].startswith("thelake-test-"):
            return subprocess.CompletedProcess(command, 0, f"container-id {run_id}\n", "")
        if command[:3] == ["docker", "inspect", "--format"]:
            return subprocess.CompletedProcess(command, 0, f"{run_id}\n", "")
        if command[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(command, 0, "test log\n", "")
        if command[:3] == ["docker", "rm", "--force"]:
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="interrupted-container",
        artifact_dir=artifact_dir,
        state_file=None,
        image="grafana/loki:test",
        docker_option=[],
        published_port=[],
        container_command=[],
    )

    try:
        test_stack._start_container(args)
    except RuntimeError as error:
        assert "container was removed" in str(error)
    else:
        raise AssertionError("an interrupted container start must be reported")

    state = json.loads((artifact_dir / "stack.json").read_text())
    assert state["status"] == "stopped"
    assert state["container_id"] == "container-id"
    assert "test log" in (artifact_dir / "container-logs.txt").read_text()
    assert any(command[-2:] == ["--force", "container-id"] for command in commands)
    assert not Path(state["scratch_dir"]).exists()


def test_container_port_discovery_interrupt_stops_saved_container(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "artifacts"
    commands = []
    run_id = None

    def run(command, *, check=True):
        nonlocal run_id
        commands.append(command)
        if command[:2] == ["docker", "run"]:
            run_id = next(value.split("=", 1)[1] for value in command if value.startswith(f"{test_stack.RUN_LABEL}="))
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        if command[:2] == ["docker", "port"]:
            state = json.loads((artifact_dir / "stack.json").read_text())
            assert state["container_id"] == "container-id"
            assert state["status"] == "starting"
            raise KeyboardInterrupt
        if command[:3] == ["docker", "inspect", "--format"]:
            return subprocess.CompletedProcess(command, 0, f"{run_id}\n", "")
        if command[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(command, 0, "test log\n", "")
        if command[:3] == ["docker", "rm", "--force"]:
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="interrupted-port-discovery",
        artifact_dir=artifact_dir,
        state_file=None,
        image="grafana/loki:test",
        docker_option=[],
        published_port=["3100"],
        container_command=[],
    )

    try:
        test_stack._start_container(args)
    except RuntimeError as error:
        assert "container was removed" in str(error)
    else:
        raise AssertionError("interrupted port discovery must be reported")

    state = json.loads((artifact_dir / "stack.json").read_text())
    assert state["status"] == "stopped"
    assert state["container_id"] == "container-id"
    assert any(command[-2:] == ["--force", "container-id"] for command in commands)
    assert not Path(state["scratch_dir"]).exists()


def test_container_start_failure_removes_partially_created_container(tmp_path, monkeypatch):
    artifact_dir = tmp_path / "artifacts"
    commands = []
    run_id = None

    def run(command, *, check=True):
        nonlocal run_id
        commands.append(command)
        if command[:2] == ["docker", "run"]:
            run_id = next(
                value.split("=", 1)[1]
                for value in command
                if value.startswith(f"{test_stack.RUN_LABEL}=")
            )
            return subprocess.CompletedProcess(command, 125, "", "port allocation failed")
        if command[:3] == ["docker", "inspect", "--format"] and command[-1].startswith(
            "thelake-test-"
        ):
            return subprocess.CompletedProcess(command, 0, f"container-id {run_id}\n", "")
        if command[:3] == ["docker", "inspect", "--format"]:
            return subprocess.CompletedProcess(command, 0, f"{run_id}\n", "")
        if command[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(command, 0, "partial container log\n", "")
        if command[:3] == ["docker", "rm", "--force"]:
            return subprocess.CompletedProcess(command, 0, "container-id\n", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="failed-container-start",
        artifact_dir=artifact_dir,
        state_file=None,
        image="grafana/loki:test",
        docker_option=[],
        published_port=[],
        container_command=[],
    )

    try:
        test_stack._start_container(args)
    except RuntimeError as error:
        assert "failed to start" in str(error)
    else:
        raise AssertionError("a failed container start must be reported")

    state = json.loads((artifact_dir / "stack.json").read_text())
    assert state["status"] == "stopped"
    assert state["container_id"] == "container-id"
    assert "port allocation failed" in (artifact_dir / "container-start.txt").read_text()
    assert "partial container log" in (artifact_dir / "container-logs.txt").read_text()
    assert any(command[-2:] == ["--force", "container-id"] for command in commands)
    assert not Path(state["scratch_dir"]).exists()


def test_compose_start_uses_unique_project_and_records_mapped_ports(tmp_path, monkeypatch):
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        if command[:2] == ["docker", "ps"] or command[:3] in (
            ["docker", "volume", "ls"],
            ["docker", "network", "ls"],
        ):
            return subprocess.CompletedProcess(command, 0, "", "")
        if "up" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        if "port" in command:
            return subprocess.CompletedProcess(command, 0, "127.0.0.1:43210\n", "")
        if "ps" in command or "logs" in command or "down" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="lisa-pytest",
        artifact_dir=tmp_path / "artifacts",
        state_file=None,
        compose_file=[compose_file],
        env_file=[],
        service=["lisa-postgres"],
        published_port=["lisa-postgres:5432"],
        wait=True,
    )

    state = test_stack._start_compose(args)

    assert state["status"] == "running"
    assert state["ports"] == {"lisa-postgres:5432": ["127.0.0.1:43210"]}
    assert state["project_name"].startswith("thelake-test-lisa-pytest-")
    startup = next(command for command in commands if "up" in command)
    assert "--wait" in startup
    assert "lisa-postgres" in startup
    test_stack._stop(Path(state["state_file"]))
    assert not Path(state["scratch_dir"]).exists()


def test_compose_start_failure_cleans_stack_but_keeps_start_log(tmp_path, monkeypatch):
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        if command[:2] == ["docker", "ps"] or command[:3] in (
            ["docker", "volume", "ls"],
            ["docker", "network", "ls"],
        ):
            return subprocess.CompletedProcess(command, 0, "", "")
        if "up" in command:
            return subprocess.CompletedProcess(command, 1, "", "port already in use")
        if "ps" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        if "logs" in command:
            return subprocess.CompletedProcess(command, 0, "partial startup output", "")
        if "down" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="failed-start",
        artifact_dir=tmp_path / "artifacts",
        state_file=None,
        compose_file=[compose_file],
        env_file=[],
        service=["postgres"],
        published_port=[],
        wait=False,
    )

    try:
        test_stack._start_compose(args)
    except RuntimeError as error:
        assert "failed to start" in str(error)
    else:
        raise AssertionError("failed Compose startup should be reported")

    artifact_dir = tmp_path / "artifacts"
    assert "port already in use" in (artifact_dir / "compose-start.txt").read_text()
    assert "partial startup output" in (artifact_dir / "compose-logs.txt").read_text()
    state = json.loads((artifact_dir / "stack.json").read_text())
    assert state["status"] == "failed"
    assert any("down" in command and "--volumes" in command for command in commands)


def test_compose_start_failure_keeps_cleanup_retryable(tmp_path, monkeypatch):
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    artifact_dir = tmp_path / "artifacts"
    commands = []
    cleanup_attempts = 0
    container_queries = 0

    def run(command, *, check=True):
        nonlocal cleanup_attempts, container_queries
        commands.append(command)
        if command[:2] == ["docker", "ps"]:
            container_queries += 1
            if container_queries == 1:
                return subprocess.CompletedProcess(command, 0, "", "")
            return subprocess.CompletedProcess(command, 0, "partial-container\n", "")
        if command[:3] in (
            ["docker", "volume", "ls"],
            ["docker", "network", "ls"],
        ):
            return subprocess.CompletedProcess(command, 0, "", "")
        if "up" in command:
            return subprocess.CompletedProcess(command, 1, "", "startup failed")
        if "ps" in command or "logs" in command:
            return subprocess.CompletedProcess(command, 0, "partial startup output", "")
        if "down" in command:
            cleanup_attempts += 1
            if cleanup_attempts == 1:
                return subprocess.CompletedProcess(command, 1, "", "Docker unavailable")
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="failed-start-retry",
        artifact_dir=artifact_dir,
        state_file=None,
        compose_file=[compose_file],
        env_file=[],
        service=["postgres"],
        published_port=[],
        wait=False,
    )

    try:
        test_stack._start_compose(args)
    except RuntimeError as error:
        assert "retry with" in str(error)
    else:
        raise AssertionError("failed Compose startup should be reported")

    state_file = artifact_dir / "stack.json"
    state = json.loads(state_file.read_text())
    scratch_dir = Path(state["scratch_dir"])
    # A partial startup can outlive a failed first teardown; `stop` must retry
    # only the resources that this run recorded and keep scratch until then.
    assert state["status"] == "cleanup-required"
    assert state["compose_resources"]["containers"] == ["partial-container"]
    assert "Docker unavailable" in (artifact_dir / "compose-cleanup.txt").read_text()
    assert scratch_dir.exists()

    stopped = test_stack._stop(state_file)

    assert stopped["status"] == "stopped"
    assert not scratch_dir.exists()
    assert cleanup_attempts == 2


def test_compose_teardown_rejects_replayed_run_state(tmp_path):
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    state_file = artifact_dir / "stack.json"
    state_file.write_text(
        json.dumps(
            {
                "run_id": "123456789abc",
                "suite": "sample",
                "project_name": "thelake-test-sample-123456789abc",
                "artifact_dir": str(artifact_dir),
                "scratch_dir": str(tmp_path / "scratch"),
                "state_file": str(state_file),
                "kind": "compose",
                "status": "stopped",
            }
        ),
        encoding="utf-8",
    )

    try:
        test_stack._stop(state_file)
    except RuntimeError as error:
        assert "not active" in str(error)
    else:
        raise AssertionError("a stopped stack must not be torn down a second time")


def _container_stack_state(tmp_path):
    run_id = "123456789abc"
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    scratch_dir = Path(tempfile.mkdtemp(prefix=f"thelake-test-{run_id}-"))
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
                "kind": "container",
                "container_id": "container-id",
                "status": "running",
            }
        ),
        encoding="utf-8",
    )
    return state_file, artifact_dir, scratch_dir


def test_container_teardown_marks_already_removed_container_stopped(tmp_path, monkeypatch):
    state_file, artifact_dir, scratch_dir = _container_stack_state(tmp_path)
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, "", "Error: No such object: container-id")

    monkeypatch.setattr(test_stack, "_run", run)

    state = test_stack._stop(state_file)

    assert state["status"] == "stopped"
    assert len(commands) == 1 and commands[0][1] == "inspect"
    assert (artifact_dir / "container-logs.txt").read_text() == ""
    assert not scratch_dir.exists()


def test_container_teardown_does_not_remove_after_inspect_error(tmp_path, monkeypatch):
    state_file, _, scratch_dir = _container_stack_state(tmp_path)
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, "", "Docker daemon unavailable")

    monkeypatch.setattr(test_stack, "_run", run)

    try:
        test_stack._stop(state_file)
    except RuntimeError as error:
        assert "Docker daemon unavailable" in str(error)
    else:
        raise AssertionError("Docker inspect errors must prevent container removal")

    assert len(commands) == 1
    assert scratch_dir.exists()


def test_compose_teardown_recovers_interrupted_start(tmp_path, monkeypatch):
    run_id = "123456789abc"
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    scratch_dir = Path(tempfile.mkdtemp(prefix=f"thelake-test-{run_id}-"))
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
                "status": "starting",
            }
        ),
        encoding="utf-8",
    )
    commands = []

    def run(command, *, check=True):
        commands.append(command)
        if command[:2] == ["docker", "ps"]:
            return subprocess.CompletedProcess(command, 0, "partial-container\n", "")
        if command[:3] in (["docker", "volume", "ls"], ["docker", "network", "ls"]):
            return subprocess.CompletedProcess(command, 0, "", "")
        if "logs" in command:
            return subprocess.CompletedProcess(command, 0, "interrupted startup\n", "")
        if "down" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        if "ps" in command:
            return subprocess.CompletedProcess(command, 0, "partial stack\n", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)

    state = test_stack._stop(state_file)

    assert state["status"] == "stopped"
    assert state["compose_resources"]["containers"] == ["partial-container"]
    assert not scratch_dir.exists()


def test_compose_start_interrupt_cleans_its_stack(tmp_path, monkeypatch):
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    artifact_dir = tmp_path / "artifacts"
    commands = []
    preflight_queries = 0

    def run(command, *, check=True):
        nonlocal preflight_queries
        commands.append(command)
        if "up" in command:
            raise KeyboardInterrupt
        if command[:2] == ["docker", "ps"]:
            if preflight_queries == 0:
                preflight_queries += 1
                return subprocess.CompletedProcess(command, 0, "", "")
            return subprocess.CompletedProcess(command, 0, "partial-container\n", "")
        if command[:3] in (["docker", "volume", "ls"], ["docker", "network", "ls"]):
            return subprocess.CompletedProcess(command, 0, "", "")
        if "ps" in command or "logs" in command or "down" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(f"unexpected test-stack command: {command}")

    monkeypatch.setattr(test_stack, "_run", run)
    args = Namespace(
        suite="interrupted-start",
        artifact_dir=artifact_dir,
        state_file=None,
        compose_file=[compose_file],
        env_file=[],
        service=["postgres"],
        published_port=[],
        wait=True,
    )

    try:
        test_stack._start_compose(args)
    except RuntimeError as error:
        assert "was interrupted" in str(error)
    else:
        raise AssertionError("an interrupted startup must be reported")

    state = json.loads((artifact_dir / "stack.json").read_text())
    assert state["status"] == "failed"
    assert any("down" in command for command in commands)
    assert not Path(state["scratch_dir"]).exists()
