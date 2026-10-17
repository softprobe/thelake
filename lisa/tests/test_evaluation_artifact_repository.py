import pytest

from lisa.context import WorkspaceContext
from lisa.repositories.evaluation_artifacts import EvaluationArtifactRepository
from lisa.views.evaluations import render_evaluation_context

WORKSPACE_ID = "8cd71112-21e0-4d45-a8cc-9ec553879df2"


def test_evaluation_artifact_repository_validates_run_directory_and_saves_projection(tmp_path):
    repository = EvaluationArtifactRepository(WorkspaceContext(WORKSPACE_ID, tmp_path))
    run_id = "run-123"
    run_dir = repository.prepare_run_directory(f"work-items/{run_id}", run_id)
    inputs = {
        "session_id": "session-1",
        "trace_id": "trace-1",
        "policy_id": "P1.1",
        "policy_name": "Refund policy",
        "target_agent_name": "refund-agent",
    }

    repository.save_inputs(run_dir, inputs, render_evaluation_context(inputs))

    assert (run_dir / "inputs.json").read_text() == (
        '{\n  "policy_id": "P1.1",\n'
        '  "policy_name": "Refund policy",\n'
        '  "session_id": "session-1",\n'
        '  "target_agent_name": "refund-agent",\n'
        '  "trace_id": "trace-1"\n}'
    )
    assert "Policy: Refund policy" in (run_dir / "context.md").read_text()


def test_evaluation_artifact_repository_rejects_another_work_items_path(tmp_path):
    repository = EvaluationArtifactRepository(WorkspaceContext(WORKSPACE_ID, tmp_path))

    with pytest.raises(ValueError, match="does not match its work item"):
        repository.prepare_run_directory("work-items/other", "run-123")


def test_evaluation_artifact_repository_rejects_symlinked_run_directory(tmp_path):
    repository = EvaluationArtifactRepository(WorkspaceContext(WORKSPACE_ID, tmp_path))
    real_directory = tmp_path / "outside"
    real_directory.mkdir()
    home = repository.context.home()
    work_items = home / "work-items"
    work_items.mkdir(parents=True)
    (work_items / "run-123").symlink_to(real_directory)

    with pytest.raises(ValueError, match="cannot be a symlink"):
        repository.prepare_run_directory("work-items/run-123", "run-123")


def test_evaluation_artifact_repository_rejects_symlinked_lock_directory(tmp_path):
    repository = EvaluationArtifactRepository(WorkspaceContext(WORKSPACE_ID, tmp_path))
    home = repository.context.home()
    home.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (home / "evaluation-locks").symlink_to(outside)

    with (
        pytest.raises(ValueError, match="lock path"),
        repository.lock("policy-hash", "session"),
    ):
        pass
