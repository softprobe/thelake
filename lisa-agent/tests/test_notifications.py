import io

import pytest

from thelake_lisa import thelake_client
from thelake_lisa.context import WorkspaceContext
from thelake_lisa.services import build_services


@pytest.fixture(autouse=True)
def isolated_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))


def test_failure_notification_routes_and_deduplicates_legacy_check(monkeypatch, tmp_path):
    import json

    workspace_id = "11111111-1111-4111-8111-111111111111"
    evaluator = {
        "evaluator_id": "legacy-check",
        "version": 3,
        "name": "Refund eligibility",
        "target_agent_name": "support-agent",
        "slack_channel_id": None,
        "slack_thread_ts": "thread-from-unbound-channel",
    }
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "test-bot-token")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "workspace-alerts")
    services = build_services(WorkspaceContext.from_environment())
    services.failures.record(evaluator, "session-1", "trace-1", "Missing eligibility check.", None)
    monkeypatch.setattr(
        services.client,
        "get",
        lambda *_args, **_kwargs: {
            "session_id": "session-1",
            "span_count": 1,
            "agent_names": ["support-agent"],
            "spans": [{"trace_id": "trace-1"}],
            "scores": [
                {
                    "score_id": "legacy-score",
                    "trace_id": "trace-1",
                    "config_id": "evaluator:legacy-check:v3",
                    "source": "evaluator",
                    "string_value": "fail",
                    "comment": "Missing eligibility check.",
                }
            ],
        },
    )
    requests = []

    def fake_urlopen(request, timeout):
        requests.append((json.loads(request.data), timeout))
        return io.BytesIO(json.dumps({"ok": True, "ts": "1710000000.000001"}).encode())

    monkeypatch.setattr(thelake_client.urllib.request, "urlopen", fake_urlopen)

    result = services.evaluations.notify_evaluation_failure(
        "support-agent", "Refund eligibility", "session-1"
    )
    repeated_result = services.evaluations.notify_evaluation_failure(
        "support-agent", "Refund eligibility", "session-1"
    )

    assert result["status"] == "sent"
    assert requests[0][0]["channel"] == "workspace-alerts"
    assert "thread_ts" not in requests[0][0]
    assert "session-1" in requests[0][0]["text"]
    assert repeated_result == {"status": "already_sent", "session_id": "session-1"}
    assert len(requests) == 1
