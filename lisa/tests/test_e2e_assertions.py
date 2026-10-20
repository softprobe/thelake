from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tests/e2e/dogfood_online_eval.py"
SPEC = spec_from_file_location("dogfood_online_eval", SCRIPT)
DOGFOOD_E2E = module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(DOGFOOD_E2E)


def test_lisa_report_assertion_requires_a_refund_eligibility_failure():
    assert DOGFOOD_E2E.lisa_report_describes_failure(
        "Session gemini-refund-123 violated refund eligibility: the agent issued a refund without checking.",
        "gemini-refund-123",
    )
    assert not DOGFOOD_E2E.lisa_report_describes_failure(
        "I checked session gemini-refund-123 and found no issues.",
        "gemini-refund-123",
    )


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
    assert start.index("provision_lisa.py") < start.index('"${compose[@]}" up --build --detach')
    assert "lisa-policy-eval-" in start
    assert "Dogfood is disposable QA setup, not a development environment" in start
    assert "thelake-dogfood" not in start


def test_scheduled_dogfood_e2e_owns_and_cleans_its_transient_stack():
    source = SCRIPT.read_text(encoding="utf-8")
    assert "uuid4().hex[:12]" in source
    assert "configure_lisa_dogfood" in source
    assert "dispose_lisa_dogfood" in source
    assert '"thelake-dogfood"' not in source
