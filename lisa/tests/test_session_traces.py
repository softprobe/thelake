from __future__ import annotations

import json

import pytest

from lisa.repositories.session_traces import (
    SessionTraceConflict,
    SessionTraceStore,
)


def _attribute(key: str, value: str) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def _bundle(*, session_id: str = "session-with-safe-id", agent: str = "checkout-agent") -> dict:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        _attribute("service.name", agent),
                        _attribute("sp.agent.name", agent),
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "0123456789abcdef0123456789abcdef",
                                "spanId": "0123456789abcdef",
                                "name": "agent turn",
                                "startTimeUnixNano": "1761000000000000000",
                                "endTimeUnixNano": "1761000001000000000",
                                "attributes": [
                                    _attribute("sp.session.id", session_id),
                                    _attribute("sp.input", '{"messages": [{"role":"user","content":"hello"}]}'),
                                    _attribute("sp.output", '{"messages": [{"role":"assistant","content":"hi"}]}'),
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def test_session_bundle_is_stored_under_readable_session_path_and_loaded(tmp_path):
    bundle = _bundle()
    store = SessionTraceStore(tmp_path)

    stored = store.persist(bundle)

    expected_dir = tmp_path / "session_traces" / "session-with-safe-id"
    assert stored.session_id == "session-with-safe-id"
    assert stored.agent_name == "checkout-agent"
    assert stored.path == expected_dir
    assert json.loads((expected_dir / "otlp.json").read_text()) == bundle
    manifest = json.loads((expected_dir / "manifest.json").read_text())
    assert manifest["session_id"] == "session-with-safe-id"
    assert manifest["agent_name"] == "checkout-agent"
    assert store.has_manifest("session-with-safe-id")
    assert not store.has_manifest("missing-session")
    assert not store.has_manifest("../unsafe")
    assert store.load("session-with-safe-id")["session_id"] == "session-with-safe-id"


def test_session_bundle_replay_is_idempotent_but_changed_content_conflicts(tmp_path):
    store = SessionTraceStore(tmp_path)
    bundle = _bundle()

    first = store.persist(bundle)
    replay = store.persist(bundle)

    assert replay.path == first.path
    changed = _bundle()
    changed["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"] = "changed"
    with pytest.raises(SessionTraceConflict):
        store.persist(changed)


@pytest.mark.parametrize(
    "bundle",
    [
        {"resourceSpans": []},
        {
            "resourceSpans": [
                {
                    "resource": {"attributes": []},
                    "scopeSpans": [{"spans": [{"traceId": "a" * 32, "spanId": "b" * 16}]}],
                }
            ]
        },
    ],
)
def test_session_bundle_requires_spans_with_consistent_identity(tmp_path, bundle):
    store = SessionTraceStore(tmp_path)
    with pytest.raises(ValueError):
        store.persist(bundle)


def test_session_identity_uses_lake_precedence_and_rejects_mixed_sessions(tmp_path):
    store = SessionTraceStore(tmp_path)
    bundle = _bundle(session_id="canonical")
    span = bundle["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    span["attributes"] = [
        _attribute("gen_ai.conversation.id", "conversation"),
    ]
    assert store.persist(bundle).session_id == "conversation"

    other = json.loads(json.dumps(bundle))
    other["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"] = [
        _attribute("sp.session.id", "one"),
        _attribute("sp.agent.name", "checkout-agent"),
    ]
    other["resourceSpans"][0]["scopeSpans"][0]["spans"].append(
        {
            "traceId": "1123456789abcdef0123456789abcdef",
            "spanId": "1123456789abcdef",
            "attributes": [
                _attribute("sp.session.id", "two"),
                _attribute("sp.agent.name", "checkout-agent"),
            ],
        }
    )
    with pytest.raises(ValueError, match="same session"):
        store.persist(other)


def test_session_bundle_rejects_unsafe_id_and_partial_otlp_structure(tmp_path):
    store = SessionTraceStore(tmp_path)
    with pytest.raises(ValueError, match="unsafe"):
        store.persist(_bundle(session_id="../escape"))
    malformed = _bundle()
    malformed["resourceSpans"][0]["resource"] = None
    with pytest.raises(ValueError, match="resource object"):
        store.persist(malformed)


def test_manifest_contains_search_summary_without_loading_trace(tmp_path, monkeypatch):
    store = SessionTraceStore(tmp_path)
    store.persist(_bundle())

    def unexpected_load(_session_id):
        raise AssertionError("Session search must not read full OTLP bundles")

    monkeypatch.setattr(store, "load", unexpected_load)
    from lisa.services.sessions import SessionService

    result = SessionService(store).search(from_time="2020-01-01T00:00:00Z")
    assert result["items"][0]["session_id"] == "session-with-safe-id"


def test_session_search_reports_distinct_trace_count_for_bundle(tmp_path):
    bundle = _bundle()
    span = json.loads(json.dumps(bundle["resourceSpans"][0]["scopeSpans"][0]["spans"][0]))
    span["traceId"] = "1123456789abcdef0123456789abcdef"
    span["spanId"] = "1123456789abcdef"
    bundle["resourceSpans"][0]["scopeSpans"][0]["spans"].append(span)
    store = SessionTraceStore(tmp_path)
    store.persist(bundle)

    from lisa.services.sessions import SessionService

    result = SessionService(store).search(from_time="2020-01-01T00:00:00Z")
    assert result["items"][0]["trace_count"] == 2
    assert result["items"][0]["span_count"] == 2


@pytest.mark.parametrize("value", [{"arrayValue": None}, {"kvlistValue": None}])
def test_malformed_nested_anyvalue_is_rejected(tmp_path, value):
    body = _bundle()
    body["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"].append(
        {"key": "bad", "value": value}
    )
    with pytest.raises(ValueError, match="OTLP"):
        SessionTraceStore(tmp_path).persist(body)
