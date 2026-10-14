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
    lisa_environment = compose["services"]["lisa-agent"]["environment"]
    assert lisa_environment["LISA_SLACK_CHANNEL_ID"] == "${LISA_SLACK_CHANNEL_ID:-}"
