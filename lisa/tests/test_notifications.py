import io

from lisa import thelake_client
from lisa.bootstrap import build_services


def test_failure_notification_routes_and_deduplicates(monkeypatch, lisa_test_context):
    import json

    workspace_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace_id)
    monkeypatch.setenv("LISA_SLACK_BOT_TOKEN", "test-bot-token")
    monkeypatch.setenv("LISA_SLACK_CHANNEL_ID", "workspace-alerts")
    services = build_services(
        lisa_test_context(slack_token="test-bot-token", slack_channel="workspace-alerts")
    )
    policy = {
        "name": "Refund eligibility",
        "target_agent_name": "support-agent",
        "policy_hash": "policy-hash-1",
        "policy_id": "P1.1",
        "scope": "agent",
        "criteria": "Check eligibility before refunding.",
        "slack_channel_id": "workspace-alerts",
        "slack_thread_ts": None,
    }
    services.failures.record(policy, "session-1", "trace-1", "Missing eligibility check.", None)
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
                    "score_id": "sample-score",
                    "trace_id": "trace-1",
                    "config_id": None,
                    "source": "evaluator",
                    "string_value": "fail",
                    "comment": "Missing eligibility check.",
                    "metadata": {"policy_hash": "policy-hash-1"},
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
