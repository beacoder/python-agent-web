"""Rate limiting on auth (per-IP) and run (per-user) endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.infra.config import get_settings


class TestAuthRateLimit:
    def test_login_throttled_after_limit(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "rate_limit_auth", 3)
        # wrong creds return 401; the limiter counts every attempt
        seen = [
            client.post(
                "/auth/login", json={"email": "x@y.com", "password": "password-1"}
            ).status_code
            for _ in range(4)
        ]
        assert seen[:3] == [401, 401, 401]
        assert seen[3] == 429

    def test_429_carries_retry_after(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "rate_limit_auth", 1)
        client.post("/auth/login", json={"email": "a@b.com", "password": "password-1"})
        res = client.post("/auth/login", json={"email": "a@b.com", "password": "password-1"})
        assert res.status_code == 429
        assert res.headers.get("Retry-After") is not None
        assert "rate limit" in res.json()["detail"]

    def test_register_is_limited_too(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "rate_limit_auth", 2)
        codes = [
            client.post(
                "/auth/register", json={"email": f"u{i}@e.com", "password": "password-1"}
            ).status_code
            for i in range(3)
        ]
        assert codes[:2] == [201, 201]
        assert codes[2] == 429


class TestRunRateLimit:
    def _runner_patch(self, monkeypatch):
        from app.controllers import manager as manager_mod
        from app.controllers.runner import ExecResult, Runner

        class QuietRunner(Runner):
            def create(self, user_id, conversation_id):
                return "sbx"

            def destroy(self, sandbox_id):
                pass

            def exec_run(self, sandbox_id, prompt, run_id, on_line=None, timeout=None):
                return ExecResult(
                    exit_code=0,
                    stdout='{"type": "result", "answer": "ok", "errors": []}\n',
                    stderr="",
                )

            def reap_idle(self, ttl_seconds):
                return []

        monkeypatch.setattr(manager_mod, "_controller", None)
        monkeypatch.setattr(manager_mod, "get_runner", lambda: QuietRunner(), raising=True)

    def test_run_submissions_throttled_per_user(
        self, client: TestClient, auth_headers: dict, monkeypatch
    ) -> None:
        self._runner_patch(monkeypatch)
        monkeypatch.setattr(get_settings(), "rate_limit_runs", 2)
        cid = client.post("/conversations", json={"title": "t"}, headers=auth_headers).json()["id"]
        codes = [
            client.post(
                f"/conversations/{cid}/runs", json={"prompt": f"p{i}"}, headers=auth_headers
            ).status_code
            for i in range(3)
        ]
        # 202 accepted until the per-user window fills, then 429
        assert codes.count(429) == 1
        assert codes[2] == 429

        # let the accepted runs' worker threads finalize before the next
        # test resets the DB engine (a late finalize would hit a dead engine)
        import time

        from app.controllers.manager import get_controller

        ctrl = get_controller()
        deadline = time.time() + 10
        while ctrl._active and time.time() < deadline:
            time.sleep(0.02)


class TestLimiterUnit:
    def test_sliding_window_allows_then_blocks(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter

        lim = SlidingWindowLimiter()
        assert lim.check("k", 2, 60)[0] is True
        assert lim.check("k", 2, 60)[0] is True
        allowed, retry = lim.check("k", 2, 60)
        assert allowed is False and retry > 0

    def test_keys_are_independent(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter

        lim = SlidingWindowLimiter()
        assert lim.check("a", 1, 60)[0] is True
        assert lim.check("b", 1, 60)[0] is True  # different key, own budget
        assert lim.check("a", 1, 60)[0] is False

    def test_reset_clears_counters(self) -> None:
        from app.infra.ratelimit import SlidingWindowLimiter

        lim = SlidingWindowLimiter()
        lim.check("k", 1, 60)
        assert lim.check("k", 1, 60)[0] is False
        lim.reset()
        assert lim.check("k", 1, 60)[0] is True


@pytest.fixture(autouse=True)
def _reset_limiter():
    from app.infra.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()
