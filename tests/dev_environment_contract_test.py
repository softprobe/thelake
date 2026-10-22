from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_standard_dev_stack_starts_thelake_and_lisa_together():
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "dev-up" in makefile
    assert "dev-down" in makefile
    assert "_ensure-dist" in makefile
    assert "lisa-provision:" in compose
    assert "platform: linux/amd64" in compose
    assert "service_completed_successfully" in compose
    assert "postgresql://lisa_writer:" in compose
    assert "LISA_SERVICE_TOKEN" in compose
    assert "LISA_QA_ENABLED" not in compose
    assert "LISA_QA_CRON" not in compose
    assert "LISA_ACTIVITY_FOLLOWUPS_ENABLED" not in compose
    assert "lisa-provision" in compose and "backend_net" in compose
    assert '"127.0.0.1:8080:8080"' in compose
    assert "LISA_OWNER_USER_IDS=${LISA_OWNER_USER_IDS:-local}" in compose
    assert "LISA_EXPLORER_URL=${LISA_EXPLORER_URL-http://127.0.0.1:${THELAKE_DEV_PORT:-8090}/explorer/}" in compose
    dev_down = makefile.split("dev-down:", 1)[1].split("\n\n", 1)[0]
    assert "stop lisa otlp-backend" in dev_down
    assert " down" not in dev_down


def test_local_dev_config_uses_a_workspace_shared_by_lisa():
    config = (ROOT / "config.dev.yaml").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "workspace_scope_mode: shared" in config
    assert "metadata_schema: softprobe_dev" in config
    assert "extension_path: /app/dist/ducklake.duckdb_extension" in config
    assert "LISA_WORKSPACE_ID=${THELAKE_DEFAULT_WORKSPACE_ID" in compose
    assert "SOFTPROBE_LOCAL_ANONYMOUS=1" in compose
    assert "DUCKLAKE_METADATA_SCHEMA=softprobe_dev" in compose


def test_duplicate_interactive_quickstart_has_been_replaced_by_dogfood():
    assert not (ROOT / "examples/quickstart").exists()
    assert (ROOT / "examples/lisa-dogfood/compose.yaml").exists()
    assert (ROOT / "examples/lisa-dogfood/start.sh").exists()
    assert not (ROOT / "docs/quickstart.md").exists()
    assert "dev-up" in (ROOT / "Makefile").read_text(encoding="utf-8")


def test_dev_environment_init_preserves_existing_keys_and_generates_local_credentials(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("GEMINI_API_KEY=local-model-key\nCUSTOM_VALUE=keep-me\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/dev_env.py"), "init", "--env-file", str(env_file)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    values = dict(
        line.split("=", 1)
        for line in env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    assert values["GEMINI_API_KEY"] == "local-model-key"
    assert values["CUSTOM_VALUE"] == "keep-me"
    assert values["THELAKE_DEFAULT_WORKSPACE_ID"]
    assert values["LISA_SERVICE_TOKEN"]
    assert values["LISA_DATABASE_WRITER_PASSWORD"]
    assert values["LISA_DATABASE_READER_PASSWORD"]
    assert "LISA_QA_ENABLED" not in values
    assert "LISA_QA_CRON" not in values
    assert "LISA_ACTIVITY_FOLLOWUPS_ENABLED" not in values
    assert values["THELAKE_DEV_POSTGRES_PORT"] == "55432"
    assert values["THELAKE_LISA_PUBLIC_PORT"] == "18083"
    assert values["LISA_OWNER_USER_IDS"] == "local"
    assert "local-model-key" not in result.stdout
    assert env_file.stat().st_mode & 0o777 == 0o600


def test_dev_environment_init_is_idempotent(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("GEMINI_API_KEY=local-model-key\n", encoding="utf-8")
    command = [sys.executable, str(ROOT / "scripts/dev_env.py"), "init", "--env-file", str(env_file)]
    subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    first = env_file.read_text(encoding="utf-8")
    subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    assert env_file.read_text(encoding="utf-8") == first


def test_dev_environment_init_derives_browser_urls_from_selected_ports(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "THELAKE_DEV_PORT=19090\nTHELAKE_LISA_PUBLIC_PORT=19082\n",
        encoding="utf-8",
    )
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/dev_env.py"), "init", "--env-file", str(env_file)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    values = dict(
        line.split("=", 1)
        for line in env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    assert "LISA_PUBLIC_URL" not in values
    assert values["LISA_CORS_ORIGINS"] == "http://127.0.0.1:19090,http://localhost:19090"


def test_dev_environment_preserves_existing_port_and_url_settings(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "THELAKE_DEV_POSTGRES_PORT=5432\n"
        "THELAKE_LISA_PUBLIC_PORT=18082\n"
        "LISA_PUBLIC_URL=http://127.0.0.1:18082/v1\n",
        encoding="utf-8",
    )
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/dev_env.py"), "init", "--env-file", str(env_file)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    values = dict(
        line.split("=", 1)
        for line in env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    assert values["THELAKE_DEV_POSTGRES_PORT"] == "5432"
    assert values["THELAKE_LISA_PUBLIC_PORT"] == "18082"
    assert "LISA_PUBLIC_URL" not in values


def test_thelake_does_not_retain_lisa_service_authentication():
    auth_source = (ROOT / "src/api/auth.rs").read_text(encoding="utf-8")
    assert "LISA_THELAKE_TOKEN" not in auth_source
    assert "lisa_service_auth_for_route" not in auth_source
    assert "x-thelake-workspace-signature" not in auth_source

