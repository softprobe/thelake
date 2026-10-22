from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
def test_dogfood_passes_slack_alert_channel_to_lisa():
    compose = yaml.safe_load((ROOT / "examples/lisa-dogfood/compose.yaml").read_text())
    lisa_environment = compose["services"]["lisa"]["environment"]
    assert lisa_environment["LISA_SLACK_CHANNEL_ID"] == "${LISA_SLACK_CHANNEL_ID:-}"


def test_dogfood_configures_and_provisions_lisas_separate_database():
    compose = yaml.safe_load((ROOT / "examples/lisa-dogfood/compose.yaml").read_text())
    environment = compose["services"]["lisa"]["environment"]
    assert environment["LISA_DATABASE_URL"].startswith("postgresql://lisa_writer:")
    assert environment["LISA_DATABASE_READ_URL"].startswith("postgresql://lisa_reader:")
    assert "LISA_DATABASE_PROVISION_URL" not in environment

    start = (ROOT / "examples/lisa-dogfood/start.sh").read_text()
    assert "provision_lisa.py" in start
    assert "LISA_DATABASE_WRITER_PASSWORD" in start
    assert "THELAKE_DOGFOOD_PROJECT_NAME" in start
    assert "THELAKE_DOGFOOD_INSTANCE_ID" in start
    assert "THELAKE_DOGFOOD_DATA_PATH" in start
    assert "THELAKE_DOGFOOD_ENV_FILE" in start
    assert "LISA_DATABASE_RECREATE=1" in start
    postgres_up = start.index('"${compose[@]}" up --build --detach postgres')
    provision = start.index("provision_lisa.py")
    lisa_up = start.index('"${compose[@]}" up --build --detach lisa')
    assert postgres_up < provision < lisa_up
    assert "lisa-policy-eval-" in start
    assert "Dogfood is disposable QA, not a development environment" in start
    assert "thelake-dogfood" not in start


def test_dogfood_is_push_triggered_and_does_not_schedule_evaluations():
    compose = (ROOT / "examples/lisa-dogfood/compose.yaml").read_text(encoding="utf-8")
    assert "session-traces" in compose or "lisa" in compose
    assert "LISA_QA_CRON" not in compose
    assert "LISA_QA_ENABLED" not in compose
