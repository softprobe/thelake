import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

from lisa import app


def test_lifespan_starts_push_evaluation_without_a_scheduler(monkeypatch):
    started = asyncio.Event()
    stopped = []

    class Worker:
        async def run_forever(self, workspace_id):
            assert workspace_id == "workspace-1"
            started.set()
            await asyncio.Event().wait()

        async def shutdown(self):
            stopped.append(True)

    services = SimpleNamespace(
        slack_worker=Worker(),
        context=object(),
        activity_store=object(),
        session_evaluation_worker=None,
        conversation_checkpoint_worker=None,
    )
    application = SimpleNamespace(state=SimpleNamespace())

    @asynccontextmanager
    async def no_recording(_context):
        yield

    monkeypatch.setenv("LISA_WORKSPACE_ID", "workspace-1")
    monkeypatch.setattr(app, "initialize_backend", lambda: None)
    monkeypatch.setattr(app, "build_services", lambda _context: services)
    monkeypatch.setattr(app, "recording_maintenance", no_recording)

    async def exercise_lifespan():
        async with app.lifespan(application):
            await asyncio.wait_for(started.wait(), timeout=1)

    asyncio.run(exercise_lifespan())
    source = Path(app.__file__).read_text(encoding="utf-8")
    assert stopped == [True]
    assert "apscheduler" not in source
    assert "scheduled" not in source
    assert "LISA_ACTIVITY_FOLLOWUPS_ENABLED" not in source


def test_frontend_index_receives_the_configured_explorer_url(tmp_path, monkeypatch):
    (tmp_path / "index.html").write_text(
        '<meta name="thelake-explorer-url" content="__LISA_EXPLORER_URL__">',
        encoding="utf-8",
    )
    monkeypatch.setenv("LISA_STATIC_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_EXPLORER_URL", "https://lake.example/explorer/?a=1&b=2")

    response = app._lisa_frontend_file("index.html", spa_fallback=False)

    assert response.body == (
        b'<meta name="thelake-explorer-url" content="https://lake.example/explorer/?a=1&amp;b=2">'
    )
