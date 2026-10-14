import io
from urllib.parse import urlparse

import pytest

from thelake_lisa import thelake_client
from thelake_lisa.context import WorkspaceContext
from thelake_lisa.services import build_services


@pytest.fixture(autouse=True)
def isolated_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))


def test_thelake_api_errors_include_safe_response_details(monkeypatch):
    from urllib.error import HTTPError

    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake.test")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "unit-test-token")

    def reject_request(*_args, **_kwargs):
        raise HTTPError(
            "http://thelake.test/v1/sessions/session-1?limit=500",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"error":"evaluator_id contains invalid characters"}'),
        )

    services = build_services(WorkspaceContext.from_environment())
    monkeypatch.setattr(thelake_client.urllib.request, "urlopen", reject_request)

    with pytest.raises(RuntimeError, match="400.*evaluator_id contains invalid characters"):
        services.client.get("/v1/sessions/session-1?limit=500")


def test_get_uses_workspace_bound_service_auth(monkeypatch):
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_THELAKE_API_URL", "http://thelake.test")
    monkeypatch.setenv("LISA_THELAKE_TOKEN", "unit-test-token")
    captured = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"session_id":"session-1"}'

    def capture(request, timeout):
        captured["method"] = request.method
        captured["url"] = request.full_url
        captured["headers"] = request.headers
        captured["timeout"] = timeout
        return Response()

    services = build_services(WorkspaceContext.from_environment())
    monkeypatch.setattr(thelake_client.urllib.request, "urlopen", capture)

    result = services.client.get("/v1/sessions/session-1?limit=500")

    assert result == {"session_id": "session-1"}
    assert captured["method"] == "GET"
    assert urlparse(captured["url"]).path == "/v1/sessions/session-1"
    assert urlparse(captured["url"]).query == "limit=500"
    assert captured["headers"]["Authorization"] == "Bearer unit-test-token"
    assert captured["headers"]["X-thelake-workspace-id"] == "11111111-1111-4111-8111-111111111111"
    assert captured["timeout"] == 10
