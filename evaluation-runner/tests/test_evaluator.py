from __future__ import annotations

import pytest

from thelake_evaluation_runner import evaluator
from thelake_evaluation_runner.contracts import EvaluationRequest


def test_create_judge_model_uses_configured_gemini_credentials(monkeypatch):
    calls = []

    def fake_gemini_model(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(evaluator, "GeminiModel", fake_gemini_model, raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "google-test-key")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test-key")
    monkeypatch.setenv("GEMINI_MODEL_NAME", "gemini-test-model")

    evaluator.create_judge_model()

    assert calls == [
        {
            "model": "gemini-test-model",
            "api_key": "google-test-key",
            "temperature": 0,
        }
    ]


def test_create_judge_model_falls_back_to_gemini_api_key(monkeypatch):
    calls = []

    def fake_gemini_model(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(evaluator, "GeminiModel", fake_gemini_model, raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test-key")
    monkeypatch.delenv("GEMINI_MODEL_NAME", raising=False)

    evaluator.create_judge_model()

    assert calls == [
        {
            "model": "gemini-2.5-flash",
            "api_key": "gemini-test-key",
            "temperature": 0,
        }
    ]


def test_create_judge_model_requires_a_gemini_credential(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="required for Gemini evaluation"):
        evaluator.create_judge_model()


def test_evaluate_passes_gemini_model_to_conversational_geval(monkeypatch):
    model = object()
    calls = {}

    class FakeMetric:
        def __init__(self, **kwargs):
            calls.update(kwargs)
            self.score = 0.9
            self.reason = "The required disclosure came before confirmation."

        def measure(self, test_case):
            calls["test_case"] = test_case

    monkeypatch.setattr(evaluator, "create_judge_model", lambda: model)
    monkeypatch.setattr(evaluator, "ConversationalGEval", FakeMetric)
    request = EvaluationRequest.model_validate(
        {
            "run_id": "test-run",
            "evaluator": {
                "evaluator_id": "connected-disclosure",
                "version": 1,
                "name": "Connected itinerary disclosure",
                "criteria": "Warn about the onward flight before cancellation.",
            },
            "evidence": {
                "trace_id": "test-trace",
                "events": [
                    {
                        "sequence": 0,
                        "span_id": "user-span",
                        "timestamp": "2026-10-08T00:00:00Z",
                        "kind": "user_message",
                        "content": "Cancel my first flight.",
                    },
                    {
                        "sequence": 1,
                        "span_id": "assistant-span",
                        "timestamp": "2026-10-08T00:00:01Z",
                        "kind": "assistant_message",
                        "content": "Your onward connection is affected; should I proceed?",
                    },
                ],
                "complete": True,
                "tool_events_captured": False,
            },
        }
    )

    result = evaluator.evaluate(request)

    assert calls["model"] is model
    assert calls["name"] == "Connected itinerary disclosure"
    assert result.status == "pass"
