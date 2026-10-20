import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import lisa.app as app
from lisa.app import activity_followups_enabled, qa_enabled


def test_local_qa_can_be_disabled_without_disabling_chat(monkeypatch):
    monkeypatch.setenv("LISA_QA_ENABLED", "0")
    assert not qa_enabled()


def test_qa_remains_enabled_by_default(monkeypatch):
    monkeypatch.delenv("LISA_QA_ENABLED", raising=False)
    assert qa_enabled()


def test_activity_followups_remain_enabled_when_scheduled_qa_is_disabled(monkeypatch):
    monkeypatch.setenv("LISA_QA_ENABLED", "0")
    monkeypatch.delenv("LISA_ACTIVITY_FOLLOWUPS_ENABLED", raising=False)
    assert not qa_enabled()
    assert activity_followups_enabled()


def test_disabling_scheduled_qa_keeps_slack_event_worker(monkeypatch):
    started = asyncio.Event()
    stopped = []

    class Worker:
        async def run_forever(self, workspace_id):
            assert workspace_id == "workspace-1"
            started.set()
            await asyncio.Event().wait()

        async def shutdown(self):
            stopped.append(True)

    services = SimpleNamespace(slack_worker=Worker(), context=object(), activity_store=None)
    application = SimpleNamespace(state=SimpleNamespace())

    @asynccontextmanager
    async def no_recording(_context):
        yield

    monkeypatch.setenv("LISA_WORKSPACE_ID", "workspace-1")
    monkeypatch.setenv("LISA_QA_ENABLED", "0")
    monkeypatch.setattr(app, "initialize_backend", lambda: None)
    monkeypatch.setattr(app, "build_services", lambda _context: services)
    monkeypatch.setattr(app, "recording_maintenance", no_recording)

    async def exercise_lifespan():
        async with app.lifespan(application):
            await asyncio.wait_for(started.wait(), timeout=1)

    asyncio.run(exercise_lifespan())
    assert stopped == [True]

