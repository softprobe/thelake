#!/usr/bin/env python3
"""Own Docker resources for automated test runs and retain their artifacts."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_ROOT = ROOT / "target" / "test-artifacts"
RUN_LABEL = "io.arex.test-run-id"


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, capture_output=True, text=True)


def _redact(text: str) -> str:
    text = re.sub(r"(?i)(bearer\s+)[^\s\"'<>]+", r"\1[REDACTED]", text)
    return re.sub(
        r"(?i)((?:api[_-]?key|password|token|secret|authorization|cookie)\s*[:=]\s*)"
        r"[^\s,\"'<>]+",
        r"\1[REDACTED]",
        text,
    )


def _compose_prefix(state: dict) -> list[str]:
    command = ["docker", "compose"]
    for env_file in state["env_files"]:
        command.extend(["--env-file", env_file])
    command.extend(["--project-name", state["project_name"]])
    for compose_file in state["compose_files"]:
        command.extend(["--file", compose_file])
    return command


def _ensure_project_unused(project_name: str) -> None:
    for label, command in (
        ("containers", ["docker", "ps", "--all", "--quiet"]),
        ("volumes", ["docker", "volume", "ls", "--quiet"]),
        ("networks", ["docker", "network", "ls", "--quiet"]),
    ):
        result = _run(
            [*command, "--filter", f"label=com.docker.compose.project={project_name}"],
            check=False,
        )
        if result.returncode:
            raise RuntimeError(
                f"Cannot verify Docker project ownership before startup: {result.stderr.strip()}"
            )
        if result.stdout.strip():
            raise RuntimeError(
                f"Refusing to reuse existing test Compose project {project_name} ({label})"
            )


def _save_state(state: dict) -> Path:
    path = Path(state["state_file"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    return path


def _new_state(suite: str, artifact_dir: Path | None, state_file: Path | None) -> dict:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", suite):
        raise ValueError("suite must be a lowercase slug")
    run_id = uuid.uuid4().hex[:12]
    artifacts = (artifact_dir or ARTIFACT_ROOT / suite / run_id).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    target_state = (state_file or artifacts / "stack.json").resolve()
    if target_state.parent != artifacts:
        raise ValueError("Test-stack state file must be inside its artifact directory")
    if target_state.exists() or target_state.is_symlink():
        raise ValueError(f"Refusing to overwrite existing test-stack state: {target_state}")
    scratch = Path(tempfile.mkdtemp(prefix=f"thelake-test-{run_id}-"))
    return {
        "run_id": run_id,
        "suite": suite,
        "project_name": f"thelake-test-{suite}-{run_id}",
        "artifact_dir": str(artifacts),
        "scratch_dir": str(scratch),
        "state_file": str(target_state),
        "status": "starting",
    }


def _capture_compose(state: dict) -> None:
    prefix = _compose_prefix(state)
    for suffix, command in (
        ("compose-ps.txt", [*prefix, "ps", "--all"]),
        ("compose-logs.txt", [*prefix, "logs", "--no-color"]),
    ):
        result = _run(command, check=False)
        content = result.stdout + result.stderr
        (Path(state["artifact_dir"]) / suffix).write_text(_redact(content), encoding="utf-8")


def _remove_scratch(state: dict) -> None:
    scratch = Path(state["scratch_dir"]).resolve()
    prefix = Path(tempfile.gettempdir()).resolve()
    if scratch.parent != prefix or not scratch.name.startswith(f"thelake-test-{state['run_id']}-"):
        raise RuntimeError("Refusing to remove a scratch directory not owned by this test run")
    shutil.rmtree(scratch, ignore_errors=True)


def _stop(state_file: Path) -> dict:
    state_path = state_file.resolve(strict=True)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    run_id = state.get("run_id", "")
    suite = state.get("suite", "")
    if not re.fullmatch(r"[a-f0-9]{12}", run_id) or not re.fullmatch(
        r"[a-z0-9][a-z0-9_-]*", suite
    ):
        raise RuntimeError("Invalid test-run state file")
    if state.get("project_name") != f"thelake-test-{suite}-{run_id}":
        raise RuntimeError("Test-run state does not own the declared Compose project")
    artifacts = Path(state["artifact_dir"]).resolve(strict=True)
    if state_path.parent != artifacts or state_path.name != "stack.json":
        raise RuntimeError("Test-run state must stay in its artifact directory")

    if state["kind"] == "compose":
        _capture_compose(state)
        result = _run([*_compose_prefix(state), "down", "--volumes", "--remove-orphans"], check=False)
        if result.returncode:
            raise RuntimeError(
                _redact(result.stderr or result.stdout or "Docker Compose teardown failed")
            )
    elif state["kind"] == "container":
        container_id = state["container_id"]
        owner = _run(
            ["docker", "inspect", "--format", f"{{{{ index .Config.Labels \"{RUN_LABEL}\" }}}}", container_id],
            check=False,
        )
        if owner.returncode == 0 and owner.stdout.strip() != run_id:
            raise RuntimeError("Refusing to remove a container not owned by this test run")
        logs = _run(["docker", "logs", container_id], check=False)
        (artifacts / "container-logs.txt").write_text(
            _redact(logs.stdout + logs.stderr), encoding="utf-8"
        )
        removed = _run(["docker", "rm", "--force", container_id], check=False)
        if removed.returncode and owner.returncode == 0:
            raise RuntimeError(_redact(removed.stderr or "Docker container teardown failed"))
    else:
        raise RuntimeError("Unknown test-run resource kind")

    state["status"] = "stopped"
    _save_state(state)
    _remove_scratch(state)
    return state


def _start_compose(args: argparse.Namespace) -> dict:
    state = _new_state(args.suite, args.artifact_dir, args.state_file)
    state.update(
        {
            "kind": "compose",
            "compose_files": [str(Path(path).resolve(strict=True)) for path in args.compose_file],
            "env_files": [str(Path(path).resolve(strict=True)) for path in args.env_file],
            "services": args.service,
        }
    )
    try:
        _ensure_project_unused(state["project_name"])
    except (OSError, RuntimeError) as error:
        state["status"] = "refused"
        _save_state(state)
        _remove_scratch(state)
        raise error
    _save_state(state)
    prefix = _compose_prefix(state)
    command = [*prefix, "up", "--detach"]
    if args.wait:
        command.append("--wait")
    command.extend(args.service)
    result = _run(command, check=False)
    (Path(state["artifact_dir"]) / "compose-start.txt").write_text(
        _redact(result.stdout + result.stderr), encoding="utf-8"
    )
    if result.returncode:
        _capture_compose(state)
        _run([*prefix, "down", "--volumes", "--remove-orphans"], check=False)
        state["status"] = "failed"
        _save_state(state)
        _remove_scratch(state)
        raise RuntimeError(
            f"Docker Compose failed to start the test stack; see {state['artifact_dir']}/compose-start.txt"
        )
    ports = {}
    for specification in args.published_port:
        service, container_port = specification.rsplit(":", 1)
        output = _run([*prefix, "port", service, container_port], check=False)
        if output.returncode == 0 and output.stdout.strip():
            ports[specification] = output.stdout.strip().splitlines()
    state["ports"] = ports
    state["status"] = "running"
    _save_state(state)
    return state


def _start_container(args: argparse.Namespace) -> dict:
    state = _new_state(args.suite, args.artifact_dir, args.state_file)
    container_name = f"thelake-test-{args.suite}-{state['run_id']}"
    command = [
        "docker",
        "run",
        "--detach",
        "--name",
        container_name,
        "--label",
        f"{RUN_LABEL}={state['run_id']}",
    ]
    command.extend(args.docker_option)
    command.append(args.image)
    container_command = args.container_command
    if container_command[:1] == ["--"]:
        container_command = container_command[1:]
    command.extend(container_command)
    state.update({"kind": "container", "container_name": container_name})
    _save_state(state)
    result = _run(command, check=False)
    (Path(state["artifact_dir"]) / "container-start.txt").write_text(
        _redact(result.stdout + result.stderr), encoding="utf-8"
    )
    if result.returncode:
        state["status"] = "failed"
        _save_state(state)
        _remove_scratch(state)
        raise RuntimeError("Docker failed to start the test container; see container-start.txt")
    state["container_id"] = result.stdout.strip()
    ports = {}
    for container_port in args.published_port:
        output = _run(["docker", "port", state["container_id"], container_port], check=False)
        if output.returncode == 0 and output.stdout.strip():
            ports[container_port] = output.stdout.strip().splitlines()
    state["ports"] = ports
    state["status"] = "running"
    _save_state(state)
    return state


def _exec(state_file: Path, service: str, command: list[str]) -> dict:
    state = json.loads(state_file.resolve(strict=True).read_text(encoding="utf-8"))
    if state.get("kind") != "compose" or state.get("status") != "running":
        raise RuntimeError("Test commands can only run in an active Compose test stack")
    result = _run([*_compose_prefix(state), "exec", "--no-TTY", service, *command], check=False)
    return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("start-compose", "start-container"):
        start = commands.add_parser(action)
        start.add_argument("--suite", required=True)
        start.add_argument("--artifact-dir", type=Path)
        start.add_argument("--state-file", type=Path)
        if action == "start-compose":
            start.add_argument("--compose-file", type=Path, action="append", required=True)
            start.add_argument("--env-file", type=Path, action="append", default=[])
            start.add_argument("--service", action="append", default=[])
            start.add_argument("--published-port", action="append", default=[])
            start.add_argument("--wait", action="store_true")
        else:
            start.add_argument("--image", required=True)
            start.add_argument("--docker-option", action="append", default=[])
            start.add_argument("--published-port", action="append", default=[])
            start.add_argument("container_command", nargs=argparse.REMAINDER)
    stop = commands.add_parser("stop")
    stop.add_argument("--state-file", required=True, type=Path)
    execute = commands.add_parser("exec")
    execute.add_argument("--state-file", required=True, type=Path)
    execute.add_argument("--service", required=True)
    execute.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.action == "stop":
            state = _stop(args.state_file)
        elif args.action == "exec":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            state = _exec(args.state_file, args.service, command)
            if state["returncode"]:
                print(state["stderr"] or state["stdout"], file=sys.stderr, end="")
                return state["returncode"]
        elif args.action == "start-compose":
            state = _start_compose(args)
        else:
            state = _start_container(args)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(state))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
