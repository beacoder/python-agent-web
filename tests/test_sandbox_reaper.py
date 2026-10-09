"""The background idle-sandbox reaper wired into the app lifespan.

Before it existed, ``reap_idle_sandboxes`` ran exactly once at startup
— against a runner registry that is always empty at startup, since
sandboxes are in-process state.  So ``PAW_SANDBOX__TTL_SECONDS`` was
documented but never applied to a single sandbox and idle ones leaked
for the life of the process.  These tests pin the loop's contract: it
sweeps repeatedly, it is disableable, a bad pass does not kill it, and
it never bounds a run.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading

import pytest

from app.infra.config import get_settings
from app.main import _reap_idle_sandboxes_forever


class FakeController:
    """Counts sweeps; optionally raises on the first one."""

    def __init__(self, fail_first: bool = False) -> None:
        self.calls = 0
        self.fail_first = fail_first
        self.swept = threading.Event()

    def reap_idle_sandboxes(self) -> list[str]:
        self.calls += 1
        if self.fail_first and self.calls == 1:
            raise RuntimeError("db down")
        self.swept.set()
        return ["sbx_idle"]


async def _run_briefly(controller: FakeController, seconds: float = 0.25) -> None:
    task = asyncio.create_task(_reap_idle_sandboxes_forever(controller))
    await asyncio.sleep(seconds)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


class TestPeriodicReaper:
    def test_sweeps_repeatedly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(get_settings().sandbox, "reap_interval_seconds", 0.02)
        controller = FakeController()
        asyncio.run(_run_briefly(controller))
        assert controller.calls > 1  # not a one-shot

    def test_zero_interval_disables_the_sweep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(get_settings().sandbox, "reap_interval_seconds", 0)
        controller = FakeController()
        asyncio.run(_run_briefly(controller, seconds=0.1))
        assert controller.calls == 0

    def test_a_failed_sweep_does_not_kill_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(get_settings().sandbox, "reap_interval_seconds", 0.02)
        controller = FakeController(fail_first=True)
        asyncio.run(_run_briefly(controller))
        assert controller.swept.is_set()  # recovered after the exception
        assert controller.calls > 1

    def test_cancellation_stops_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Shutdown cancels the task before draining runs; it must not
        hang or swallow the cancellation."""
        monkeypatch.setattr(get_settings().sandbox, "reap_interval_seconds", 10.0)

        async def _scenario() -> bool:
            task = asyncio.create_task(_reap_idle_sandboxes_forever(FakeController()))
            await asyncio.sleep(0.01)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                return True
            return False

        assert asyncio.run(_scenario()) is True
