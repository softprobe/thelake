from types import SimpleNamespace

import pytest
from policy_cases import evaluation_request

from lisa.evaluation import evaluator, judge


def _schema_keys(value):
    if isinstance(value, dict):
        return set(value) | set().union(*(_schema_keys(item) for item in value.values()))
    if isinstance(value, list):
        return set().union(*(_schema_keys(item) for item in value)) if value else set()
    return set()


@pytest.mark.parametrize("primary", [True, False])
def test_judge_client_uses_configured_credentials(monkeypatch, primary):
    calls = []
    monkeypatch.setattr(
        evaluator.genai, "Client", lambda **kwargs: calls.append(kwargs) or object()
    )
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test-key")
    if primary:
        monkeypatch.setenv("GOOGLE_API_KEY", "google-test-key")
    else:
        monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    evaluator.create_judge_client()
    assert calls == [
        {
            "api_key": "google-test-key" if primary else "gemini-test-key",
            "http_options": {"timeout": 120_000},
        }
    ]


def test_judge_client_requires_credentials(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(evaluator.JudgeUnavailableError, match="required"):
        evaluator.create_judge_client()


@pytest.mark.parametrize(
    "fields", [{"complete": False}, {"tool_order_certain": False}, {"message_order_certain": False}]
)
def test_incomplete_or_unordered_evidence_never_calls_provider(monkeypatch, fields):
    monkeypatch.setattr(
        evaluator, "create_judge_client", lambda: pytest.fail("Must not call judge")
    )
    result = evaluator.evaluate(evaluation_request(**fields))
    assert result.status == "insufficient_evidence"
    assert "score" not in result.model_dump()


@pytest.mark.parametrize(
    "parsed",
    [
        None,
        {"status": "maybe", "rationale": "Guess"},
        {"status": "pass", "rationale": "Looks fine", "span_ids": []},
        {"status": "fail", "rationale": "Failure", "span_ids": ["invented"]},
    ],
)
def test_invalid_provider_output_is_an_execution_error(monkeypatch, parsed):
    closed = []

    class FakeClient:
        models = SimpleNamespace(generate_content=lambda **k: SimpleNamespace(parsed=parsed))

        def close(self):
            closed.append(True)

    monkeypatch.setattr(evaluator, "create_judge_client", FakeClient)
    result = judge.run_evaluation(evaluation_request().model_dump(mode="json"))
    assert result["status"] == "error"
    assert closed == [True]


def test_provider_error_does_not_leak_payload_or_credentials(monkeypatch):
    class FakeClient:
        def fail(self, **kwargs):
            raise RuntimeError('api_key="secret-value" private trace content')

        @property
        def models(self):
            return SimpleNamespace(generate_content=self.fail)

        def close(self):
            pass

    monkeypatch.setattr(evaluator, "create_judge_client", FakeClient)
    result = judge.run_evaluation(evaluation_request().model_dump(mode="json"))
    assert result["status"] == "error"
    assert "secret-value" not in str(result)
    assert "private trace content" not in str(result)


def test_judge_redacts_nested_evidence_and_uses_categorical_schema(monkeypatch):
    calls = []

    class FakeClient:
        models = SimpleNamespace(
            generate_content=lambda **kwargs: (
                calls.append(kwargs)
                or SimpleNamespace(
                    parsed={
                        "status": "uncertain",
                        "rationale": "Missing eligibility evidence",
                        "span_ids": [],
                    }
                )
            )
        )

        def close(self):
            pass

    monkeypatch.setattr(evaluator, "create_judge_client", FakeClient)
    monkeypatch.setenv("GEMINI_MODEL_NAME", "configured-model")
    request = evaluation_request()
    request.trajectory.events[0].payload = {"api_key": "private-key"}
    evaluator.evaluate(request)
    assert "private-key" not in calls[0]["contents"]
    assert calls[0]["model"] == "configured-model"
    assert calls[0]["config"].response_mime_type == "application/json"
    schema = calls[0]["config"].response_schema
    assert schema["type"] == "object"
    assert schema["properties"]["status"]["enum"] == ["pass", "fail", "uncertain"]
    assert "additionalProperties" not in schema
    assert not {"title", "maxLength", "minLength", "maxItems", "minItems"}.intersection(
        _schema_keys(schema)
    )


def test_serialized_evidence_limits_and_unapproved_requests_are_rejected():
    request = evaluation_request().model_dump(mode="json")
    request["trajectory"]["events"][0]["payload"] = {"large": "x" * 1_000_001}
    assert judge.run_evaluation(request)["status"] == "error"
    request = evaluation_request().model_dump(mode="json")
    request["policy"]["approved"] = False
    assert judge.run_evaluation(request)["status"] == "error"
