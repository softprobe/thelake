from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tests/e2e/quickstart_online_eval.py"
SPEC = spec_from_file_location("quickstart_online_eval", SCRIPT)
QUICKSTART_E2E = module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(QUICKSTART_E2E)


def test_lisa_report_assertion_requires_a_refund_eligibility_failure():
    assert QUICKSTART_E2E.lisa_report_describes_failure(
        "Session gemini-refund-123 violated refund eligibility: the agent issued a refund without checking.",
        "gemini-refund-123",
    )
    assert not QUICKSTART_E2E.lisa_report_describes_failure(
        "I checked session gemini-refund-123 and found no issues.",
        "gemini-refund-123",
    )


def test_quickstart_passes_slack_alert_channel_to_lisa():
    compose = yaml.safe_load((ROOT / "examples/quickstart/compose.yaml").read_text())
    lisa_environment = compose["services"]["lisa"]["environment"]
    assert lisa_environment["LISA_SLACK_CHANNEL_ID"] == "${LISA_SLACK_CHANNEL_ID:-}"


def test_quickstart_configures_and_provisions_lisas_separate_database():
    compose = yaml.safe_load((ROOT / "examples/quickstart/compose.yaml").read_text())
    environment = compose["services"]["lisa"]["environment"]
    assert environment["LISA_DATABASE_URL"].startswith("postgresql://lisa_writer:")
    assert environment["LISA_DATABASE_READ_URL"].startswith("postgresql://lisa_reader:")
    assert "LISA_DATABASE_PROVISION_URL" not in environment

    start = (ROOT / "examples/quickstart/start.sh").read_text()
    assert "provision_lisa.py" in start
    assert "LISA_DATABASE_WRITER_PASSWORD" in start
    assert "LISA_DATABASE_RECREATE=1" in start
    assert "migrate" not in start.lower()
    assert start.index("provision_lisa.py") < start.index('"${compose[@]}" up --build --detach')
