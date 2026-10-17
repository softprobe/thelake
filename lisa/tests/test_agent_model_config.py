from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from lisa import runtime
from lisa.backends import openhands as agent


def configure_openhands(monkeypatch):
    captured = {}

    def fake_llm(**kwargs):
        captured["llm"] = kwargs
        return object()

    def fake_agent(**kwargs):
        captured["agent"] = kwargs
        return object()

    def fake_conversation(**kwargs):
        captured["conversation"] = kwargs
        return kwargs

    monkeypatch.setattr(agent, "LLM", fake_llm)
    monkeypatch.setattr(agent, "Agent", fake_agent)
    monkeypatch.setattr(agent, "Conversation", fake_conversation)
    return captured


def test_backend_selection_defaults_to_openhands_and_rejects_unknown(monkeypatch):
    from lisa import runtime

    monkeypatch.delenv("LISA_AGENT_BACKEND", raising=False)
    assert runtime.backend_name() == "openhands"

    monkeypatch.setenv("LISA_AGENT_BACKEND", "unsupported-runtime")
    with pytest.raises(RuntimeError, match="Unsupported Lisa agent backend"):
        runtime.get_backend()


def test_backend_startup_prepares_sdk_before_serving_turns(monkeypatch):
    prepared = []

    class FakeBackend:
        def prepare(self):
            prepared.append(True)

    monkeypatch.setattr(runtime, "_openhands_backend", lambda: FakeBackend())

    runtime.initialize_backend()

    assert prepared == [True]


def test_backend_adapter_returns_common_messages_and_activities(monkeypatch, tmp_path):
    from lisa import runtime

    class FakeBackend:
        def run_turn(self, **kwargs):
            assert kwargs["workspace_id"] == "11111111-1111-4111-8111-111111111111"
            return runtime.AgentTurn(
                messages=[{"role": "assistant", "text": "Owned."}], activities=[]
            )

    monkeypatch.setenv("LISA_AGENT_BACKEND", "openhands")
    monkeypatch.setattr(runtime, "_openhands_backend", lambda: FakeBackend())
    result = runtime.run_turn(
        conversation_id="be3c93b1-d225-4ce7-8dff-06ac2924289f",
        directory=str(tmp_path),
        workspace_id="11111111-1111-4111-8111-111111111111",
        text="Investigate this failure.",
    )

    assert result.messages[0]["text"] == "Owned."


def test_policy_confirmation_is_extracted_only_from_explicit_owner_command():
    from lisa.runtime import confirmed_policy_title

    assert (
        confirmed_policy_title("Confirm policy rule: Check eligibility first")
        == "Check eligibility first"
    )
    assert confirmed_policy_title("I agree, that seems right") is None
    assert confirmed_policy_title("The customer wrote: confirm policy rule: X") is None


def test_local_model_uses_explicit_endpoint_and_never_falls_back_to_gemini(monkeypatch, tmp_path):
    captured = configure_openhands(monkeypatch)
    monkeypatch.setenv("LISA_MODEL", "ollama_chat/qwen3:4b")
    monkeypatch.setenv("LISA_BASE_URL", "http://ollama:11434")
    monkeypatch.setenv("GOOGLE_API_KEY", "gemini-secret-must-not-be-forwarded")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret-must-not-be-forwarded-either")
    monkeypatch.delenv("LISA_API_KEY", raising=False)

    agent.build_conversation(str(uuid4()), str(tmp_path), workspace_id=str(uuid4()))

    assert captured["llm"] == {
        "model": "ollama_chat/qwen3:4b",
        "api_key": None,
        "base_url": "http://ollama:11434",
        "usage_id": "lisa",
    }


def test_local_model_uses_only_its_own_optional_api_key(monkeypatch, tmp_path):
    captured = configure_openhands(monkeypatch)
    monkeypatch.setenv("LISA_MODEL", "ollama_chat/qwen3:4b")
    monkeypatch.setenv("LISA_BASE_URL", "http://ollama:11434")
    monkeypatch.setenv("LISA_API_KEY", "local-endpoint-key")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret-must-not-be-forwarded")

    agent.build_conversation(str(uuid4()), str(tmp_path), workspace_id=str(uuid4()))

    assert captured["llm"]["api_key"] == "local-endpoint-key"
    assert captured["llm"]["base_url"] == "http://ollama:11434"


def test_local_model_requires_explicit_model(monkeypatch, tmp_path):
    configure_openhands(monkeypatch)
    monkeypatch.setenv("LISA_BASE_URL", "http://ollama:11434")
    monkeypatch.delenv("LISA_MODEL", raising=False)

    with pytest.raises(RuntimeError, match="LISA_MODEL"):
        agent.build_conversation(str(uuid4()), str(tmp_path), workspace_id=str(uuid4()))


def test_gemini_default_remains_available_without_custom_endpoint(monkeypatch, tmp_path):
    captured = configure_openhands(monkeypatch)
    monkeypatch.setenv("GOOGLE_API_KEY", "gemini-test-key")
    monkeypatch.delenv("LISA_BASE_URL", raising=False)
    monkeypatch.delenv("LISA_API_KEY", raising=False)
    monkeypatch.delenv("LISA_MODEL", raising=False)

    agent.build_conversation(str(uuid4()), str(tmp_path), workspace_id=str(uuid4()))

    assert captured["llm"] == {
        "model": "gemini/gemini-2.5-flash",
        "api_key": "gemini-test-key",
        "base_url": None,
        "usage_id": "lisa",
    }


def test_decisions_api_key_is_forwarded_only_to_lisa_mcp(monkeypatch, tmp_path):
    captured = configure_openhands(monkeypatch)
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test-key")
    monkeypatch.setenv("OPENAI_API_KEY", "decisions-test-key")
    monkeypatch.setenv("LISA_POLICY_SELECTION_MIN_CONFIDENCE", "0.91")

    agent.build_conversation(str(uuid4()), str(tmp_path), workspace_id=str(uuid4()))

    configured = captured["agent"]["mcp_config"]["lisa"].env["OPENAI_API_KEY"]
    assert configured.get_secret_value() == "decisions-test-key"
    minimum_confidence = captured["agent"]["mcp_config"]["lisa"].env[
        "LISA_POLICY_SELECTION_MIN_CONFIDENCE"
    ]
    assert minimum_confidence.get_secret_value() == "0.91"
    assert captured["llm"]["api_key"] == "gemini-test-key"


@pytest.mark.parametrize(
    ("base_url", "model", "gemini_key", "expected"),
    [
        ("http://ollama:11434", "ollama_chat/qwen3:4b", None, True),
        ("http://ollama:11434", None, "gemini-test-key", False),
        (None, None, "gemini-test-key", True),
        (None, None, None, False),
    ],
)
def test_health_reports_whether_the_selected_model_is_configured(
    monkeypatch, base_url, model, gemini_key, expected
):
    from lisa.controllers import http as api

    monkeypatch.setenv("LISA_BASE_URL", base_url or "")
    monkeypatch.setenv("LISA_MODEL", model or "")
    monkeypatch.setenv("GEMINI_API_KEY", gemini_key or "")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    response = asyncio.run(api.health())
    assert response["model_configured"] is expected
